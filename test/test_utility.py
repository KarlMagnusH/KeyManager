import pytest
import pandas as pd

from keys.utility import add_bk_for_table
from CaseGen import CaseGen, BUILTINS

@pytest.fixture
def test_data():
    """Mock for pandas.read_sql function."""
    return CaseGen().add_dict(BUILTINS)

class TestBuisnessKey:
    def test_add_bk_for_table(self, test_data):
        df = test_data.combine("str_normal", "int_normal").get_df()
        table = "correct"
        df[f"bk_{table}"] = add_bk_for_table(table, df, "str_normal", "int_normal")

    def test_add_bk_for_table_mixed_col(self, test_data):
        df = test_data.combine("str_normal", "int_mixed").get_df()
        table = "correct"
        df_result = add_bk_for_table(table, df, "str_normal", "int_mixed")
        expected = (df["str_normal"].astype(str) + "||" + df["int_mixed"].astype(str))
        expected.name = "bk_correct"

        pd.testing.assert_series_equal(df_result, expected)