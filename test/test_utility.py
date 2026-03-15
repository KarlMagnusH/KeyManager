import pytest
import polars as pl

from keys.utility import add_bk_for_table
from case_gen import CaseGen, BUILTINS

@pytest.fixture
def test_data():
    return CaseGen().add_dict(BUILTINS)

class TestBuisnessKey:
    def test_add_bk_for_table(self, test_data):
        df = pl.from_pandas(test_data.combine("str_normal", "int_normal").get_df())
        table = "correct"
        df = df.with_columns(add_bk_for_table(table, df, "str_normal", "int_normal"))
        assert f"bk_{table}" in df.columns

    def test_add_bk_for_table_mixed_col(self, test_data):
        df = pl.from_pandas(test_data.combine("str_normal", "int_normal", mode="zip").get_df())
        table = "correct"
        result_series = df.with_columns(add_bk_for_table(table, df, "str_normal", "int_normal"))["bk_correct"]
        expected_series = (
            df["str_normal"].cast(pl.String) + "||" + df["int_normal"].cast(pl.String)
        ).alias("bk_correct")
        expected = df.select(expected_series).to_series()

        assert result_series.equals(expected)
