"""Generate a simple health report for configured FMCG Delta tables.

Databricks notebook usage:
    from scripts.table_health_report import build_table_health_report
    report = build_table_health_report(spark, "fmcg", ["bronze", "silver", "gold"])
    display(report.orderBy("schema_name", "table_name"))

The report reads table metadata and row counts. Row counts can scan the table,
so use it as an on-demand operational check rather than on every micro-batch.
"""
from typing import Iterable

from pyspark.sql import SparkSession


def build_table_health_report(
    spark: SparkSession,
    catalog: str,
    schemas: Iterable[str],
):
    """Return one row per table with row count and storage metadata."""
    results = []

    for schema in schemas:
        tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
        for table in tables:
            table_name = table["tableName"]
            full_name = f"`{catalog}`.`{schema}`.`{table_name}`"
            try:
                row_count = spark.table(full_name).count()
                detail = spark.sql(f"DESCRIBE DETAIL {full_name}").first().asDict()
                results.append(
                    (
                        catalog,
                        schema,
                        table_name,
                        "OK",
                        int(row_count),
                        detail.get("format"),
                        detail.get("location"),
                        None,
                    )
                )
            except Exception as exc:  # keep reporting other tables on failure
                results.append(
                    (
                        catalog,
                        schema,
                        table_name,
                        "ERROR",
                        None,
                        None,
                        None,
                        str(exc)[:500],
                    )
                )

    return spark.createDataFrame(
        results,
        [
            "catalog_name",
            "schema_name",
            "table_name",
            "status",
            "row_count",
            "table_format",
            "location",
            "error_message",
        ],
    )
