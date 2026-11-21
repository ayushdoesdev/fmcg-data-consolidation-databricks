# Data quality and table health utilities

This repository includes optional helpers for validating the FMCG Medallion
pipeline.

## Data quality checks

Import the helpers in a Databricks notebook:

```python
from scripts.data_quality_checks import (
    assert_no_nulls,
    assert_unique,
    assert_non_negative,
    summarize_quality,
)
```

Example checks after creating a Silver orders DataFrame:

```python
assert_no_nulls(df_orders, ["order_id", "order_placement_date", "product_id"])
assert_unique(df_orders, ["order_id", "product_id"])
assert_non_negative(df_orders, ["order_qty"])
display(summarize_quality(df_orders))
```

These are explicit assertions: a failing expectation raises an exception and
can stop the notebook task. Choose keys and required fields to match the
source system's business rules.

## Table health report

Inspect row counts and Delta metadata across the project schemas:

```python
from scripts.table_health_report import build_table_health_report

health = build_table_health_report(
    spark,
    catalog="fmcg",
    schemas=["bronze", "silver", "gold"],
)
display(health.orderBy("schema_name", "table_name"))
```

The report performs table row counts, which may be expensive on large tables.
Run it on demand or as a separate monitoring task rather than on every load.

## Continuous integration

The GitHub Actions workflow at `.github/workflows/python-validation.yml`
compiles the Python files on pushes and pull requests targeting `main`.
It checks syntax only; it does not execute Databricks notebooks or require
Spark credentials.
