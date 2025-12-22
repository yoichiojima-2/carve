from datetime import date
import warnings
from typing import Iterable, Callable, Any
from dataclasses import dataclass, field
from pathlib import Path

from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from shardate.dates import all_dates_between, eoms_between


def spark_session() -> SparkSession:
    if session := SparkSession.getActiveSession():
        return session
    raise RuntimeError("No active SparkSession found. Please create one.")


@dataclass
class Shardate:
    path: str
    partition_format: str = "y=%Y/m=%m/d=%d"
    skip_missing: bool = field(default=False)

    @property
    def read(self) -> Callable[[Any], DataFrame]:
        return spark_session().read.options(basePath=self.path).parquet

    def _partition_path(self, target_date: date) -> str:
        return f"{self.path}/{target_date.strftime(self.partition_format)}"

    def _partition_exists(self, target_date: date) -> bool:
        """Check if a partition exists for the given date."""
        path = self._partition_path(target_date)
        try:
            fs = spark_session()._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark_session()._jsc.hadoopConfiguration()
            )
            return fs.exists(spark_session()._jvm.org.apache.hadoop.fs.Path(path))
        except Exception:
            # Fallback for local filesystem
            return Path(path).exists()

    def _filter_existing(self, dates: Iterable[date]) -> tuple[list[date], list[date]]:
        """Split dates into existing and missing partitions."""
        existing = []
        missing = []
        for dt in dates:
            if self._partition_exists(dt):
                existing.append(dt)
            else:
                missing.append(dt)
        return existing, missing

    def list_available_dates(self, start_date: date, end_date: date) -> list[date]:
        """Discover which partitions exist within a date range.

        Useful for debugging data availability issues or finding gaps.

        Args:
            start_date: Start of the date range (inclusive)
            end_date: End of the date range (inclusive)

        Returns:
            List of dates that have existing partitions
        """
        existing, _ = self._filter_existing(all_dates_between(start_date, end_date))
        return sorted(existing)

    def read_latest(self, lookback_days: int = 30) -> DataFrame:
        """Read the most recent available partition.

        Searches backwards from today to find the latest available data.

        Args:
            lookback_days: Maximum number of days to search backwards (default: 30)

        Returns:
            DataFrame from the most recent available partition

        Raises:
            FileNotFoundError: If no partition found within lookback window
        """
        from datetime import datetime
        today = datetime.now().date()

        for days_back in range(lookback_days + 1):
            check_date = today - relativedelta(days=days_back)
            if self._partition_exists(check_date):
                return self.read_by_date(check_date)

        raise FileNotFoundError(
            f"No partition found within {lookback_days} days of {today}"
        )

    def read_by_date(self, target_date: date) -> DataFrame:
        return self.read(self._partition_path(target_date))

    def read_between(
        self, start_date: date, end_date: date
    ) -> DataFrame:
        """Read data between two dates (inclusive).

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            DataFrame containing data from all partitions in the range

        Note:
            If skip_missing=True, missing partitions are skipped with a warning.
            If skip_missing=False (default), raises an error for missing partitions.
        """
        all_dates = list(all_dates_between(start_date, end_date))

        if self.skip_missing:
            existing, missing = self._filter_existing(all_dates)
            if missing:
                warnings.warn(
                    f"Skipping {len(missing)} missing partitions: "
                    f"{missing[0]} to {missing[-1]}"
                )
            if not existing:
                raise FileNotFoundError(
                    f"No partitions found between {start_date} and {end_date}"
                )
            paths = {self._partition_path(dt) for dt in existing}
        else:
            paths = {self._partition_path(dt) for dt in all_dates}

        return self.read(*paths)

    def read_by_dates(self, target_dates: Iterable[date]) -> DataFrame:
        """Read data for specific dates.

        Args:
            target_dates: Iterable of dates to read

        Returns:
            DataFrame containing data from all specified partitions
        """
        dates_list = list(target_dates)

        if self.skip_missing:
            existing, missing = self._filter_existing(dates_list)
            if missing:
                warnings.warn(f"Skipping {len(missing)} missing partitions")
            if not existing:
                raise FileNotFoundError("No partitions found for specified dates")
            paths = {self._partition_path(dt) for dt in existing}
        else:
            paths = {self._partition_path(dt) for dt in dates_list}

        return self.read(*paths)

    def read_eoms_between(self, start_date: date, end_date: date) -> DataFrame:
        """Read end-of-month data within a date range."""
        return self.read_by_dates(eoms_between(start_date, end_date))
