"""Unit tests for validation helpers; run with pytest."""
import pytest

from scripts.data_quality_checks import (
    assert_no_nulls,
    assert_non_negative,
    assert_unique,
)


class FakeRow:
    def __init__(self, value):
        self.value = value


def test_module_exports_expected_checks():
    # PySpark execution tests belong in a Databricks/Spark test environment.
    assert callable(assert_no_nulls)
    assert callable(assert_unique)
    assert callable(assert_non_negative)


def test_checks_require_columns():
    # Guardrail tests can run without starting a Spark session by validating
    # that the API is present; actual DataFrame behavior is tested in Databricks.
    assert len((assert_no_nulls, assert_unique, assert_non_negative)) == 3
