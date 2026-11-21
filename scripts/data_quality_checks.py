"""Reusable PySpark data-quality checks for the FMCG Lakehouse.

Run from a Databricks notebook after loading a DataFrame:
    from scripts.data_quality_checks import (
        assert_no_nulls, assert_unique, assert_non_negative, summarize_quality
    )
These helpers deliberately raise AssertionError on failed expectations so they
can be used as explicit pipeline gates.
"""
from typing import Iterable, Sequence

from pyspark.sql import DataFrame, functions as F


def assert_no_nulls(df: DataFrame, columns: Sequence[str]) -> None:
    """Fail when any selected column contains NULL or an empty string."""
    missing = [
        c for c in columns
        if c not in df.columns
    ]
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {missing}")

    expressions = [
        F.sum(
            F.when(
                F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""),
                1,
            ).otherwise(0)
        ).alias(c)
        for c in columns
    ]
    counts = df.agg(*expressions).first().asDict()
    failures = {c: int(count or 0) for c, count in counts.items() if count}
    if failures:
        raise AssertionError(f"Null/blank values detected: {failures}")


def assert_unique(df: DataFrame, key_columns: Sequence[str]) -> None:
    """Fail when the supplied key does not uniquely identify each row."""
    if not key_columns:
        raise ValueError("At least one key column is required")
    missing = [c for c in key_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Key columns not found in DataFrame: {missing}")

    duplicate_groups = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_groups:
        raise AssertionError(
            f"Duplicate records found for key: {', '.join(key_columns)}"
        )


def assert_non_negative(df: DataFrame, columns: Sequence[str]) -> None:
    """Fail when numeric measures contain negative values."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {missing}")

    invalid = {}
    for column in columns:
        count = df.filter(F.col(column).cast("double") < 0).count()
        if count:
            invalid[column] = count
    if invalid:
        raise AssertionError(f"Negative measure values detected: {invalid}")


def summarize_quality(
    df: DataFrame,
    required_columns: Iterable[str] = (),
) -> DataFrame:
    """Return a compact per-column null/blank profile for a DataFrame."""
    columns = list(required_columns) or list(df.columns)
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {missing}")

    total_rows = df.count()
    expressions = [
        F.sum(
            F.when(
                F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""),
                1,
            ).otherwise(0)
        ).alias(c)
        for c in columns
    ]
    null_counts = df.agg(*expressions).first().asDict()

    spark = df.sparkSession
    rows = [
        (
            column,
            total_rows,
            int(null_counts.get(column) or 0),
            (
                0.0
                if total_rows == 0
                else round(
                    int(null_counts.get(column) or 0) / total_rows * 100, 2
                )
            ),
        )
        for column in columns
    ]
    return spark.createDataFrame(
        rows,
        ["column_name", "total_rows", "null_or_blank_rows", "null_pct"],
    )
