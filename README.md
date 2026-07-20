# shardate

[![PyPI version](https://img.shields.io/pypi/v/shardate)](https://pypi.org/project/shardate/)
[![Python versions](https://img.shields.io/pypi/pyversions/shardate)](https://pypi.org/project/shardate/)
[![License](https://img.shields.io/github/license/yoichiojima-2/shardate)](https://github.com/yoichiojima-2/shardate/blob/main/LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/yoichiojima-2/shardate/test.yml)](https://github.com/yoichiojima-2/shardate/actions)
[![Coverage](https://codecov.io/gh/yoichiojima-2/shardate/branch/main/graph/badge.svg)](https://codecov.io/gh/yoichiojima-2/shardate)
[![Code style: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

A lightweight Python library for efficiently reading year-month-day partitioned Parquet datasets with PySpark.

> **Status:** stable and feature-complete. The API is settled; maintenance releases as needed.

## Installation

```bash
pip install shardate
```

## Overview

`shardate` provides a clean, dataclass-based API for working with Parquet datasets that are partitioned in a `year/month/day` directory structure (e.g., `y=2025/m=01/d=15/`). It's built on PySpark and designed for efficient date-based filtering and data retrieval.

## Features

- **Date-based reading**: Read data for specific dates, date ranges, or collections of dates
- **End-of-month support**: Dedicated functionality for reading end-of-month data
- **Flexible partitioning**: Customizable partition format (defaults to `y=%Y/m=%m/d=%d`)
- **PySpark integration**: Seamlessly works with existing PySpark workflows
- **Type hints**: Full type annotation support for better development experience
- **Well-tested**: Comprehensive test suite ensuring reliability

## Quick Start

### Basic Usage

```python
from datetime import date
from shardate import Shardate

# Create a Shardate instance for your data path
reader = Shardate("/path/to/your/partitioned/data")

# Read data for a specific date
df = reader.read_by_date(date(2025, 1, 15))

# Read data between two dates (inclusive)
df = reader.read_between(date(2025, 1, 1), date(2025, 1, 31))

# Read data for specific dates
target_dates = [date(2025, 1, 1), date(2025, 1, 15), date(2025, 1, 31)]
df = reader.read_by_dates(target_dates)

# Read only end-of-month data within a date range
df = reader.read_eoms_between(date(2025, 1, 1), date(2025, 3, 31))
```

### Custom Partition Format

```python
# If your data uses a different partition format
reader = Shardate("/path/to/data", partition_format="year=%Y/month=%m/day=%d")
df = reader.read_by_date(date(2025, 1, 15))
```

### Working with PySpark

```python
from pyspark.sql import SparkSession
from shardate import Shardate

# Ensure you have an active SparkSession
spark = SparkSession.builder.appName("SharDate Example").getOrCreate()

# Use Shardate as normal
reader = Shardate("/path/to/data")
df = reader.read_by_date(date(2025, 1, 15))

# The returned DataFrame is a standard PySpark DataFrame
df.show()
df.filter(df.column_name == "some_value").count()
```

## API Reference

### Shardate Class

```python
@dataclass
class Shardate:
    path: str
    partition_format: str = "y=%Y/m=%m/d=%d"
```

#### Methods

- `read_by_date(target_date: date) -> DataFrame`: Read data for a specific date
- `read_between(start_date: date, end_date: date) -> DataFrame`: Read data between two dates (inclusive)  
- `read_by_dates(target_dates: Iterable[date]) -> DataFrame`: Read data for specific dates
- `read_eoms_between(start_date: date, end_date: date) -> DataFrame`: Read end-of-month data within a date range

## Data Structure Requirements

Your Parquet data should be partitioned in directories following this structure:

```
data/
├── y=2025/
│   ├── m=01/
│   │   ├── d=01/
│   │   │   └── part-*.parquet
│   │   ├── d=02/
│   │   │   └── part-*.parquet
│   │   └── d=31/
│   │       └── part-*.parquet
│   └── m=02/
│       └── ...
└── y=2024/
    └── ...
```

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/yoichiojima-2/shardate.git
cd shardate

# Install development dependencies (using uv)
uv sync --dev
```

### Testing

```bash
# Run all tests
make test

# Or use uv directly
uv run pytest -vvv
```

### Code Quality

```bash
# Run linting and formatting
make lint

# Or use uv directly
uv run ruff check --fix .
uv run ruff format .
```

### Building

```bash
# Build the package
make build

# Clean build artifacts
make clean
```

## Requirements

- **Python**: 3.12+
- **PySpark**: 4.0+
- **python-dateutil**: 2.9.0.post0+

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes and add tests
4. Run the test suite (`make test`)
5. Run code quality checks (`make lint`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Links

- **Homepage**: https://github.com/yoichiojima-2/shardate
- **Issues**: https://github.com/yoichiojima-2/shardate/issues
- **PyPI**: https://pypi.org/project/shardate/