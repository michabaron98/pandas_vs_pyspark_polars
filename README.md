# Pandas vs. PySpark vs. Polars: A Performance Comparison

Please check [my linktr.ee](https://linktr.ee/michal_baron)
This repository benchmarks the performance of Pandas, PySpark, and Polars for common data manipulation tasks.  Choosing the right tool for data processing can significantly impact execution time, especially when dealing with large datasets. This project aims to provide a practical comparison to help you make informed decisions.

## Overview

This project uses python scripts to execute and time various data manipulation operations using Pandas, PySpark, and Polars.  The scripts cover common tasks such as:

* Data loading (.csv)
* Data transformation
* Data saving (.parquet)

## Datasets

Datasets can be auto-generated. Please check `data/denerate_sample_data.py`

## Methodology

Each operation is implemented in Pandas, PySpark, and Polars.  The execution time for each operation is measured using the `timer` decorator. 