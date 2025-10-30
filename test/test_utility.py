import pytest
import pandas as pd

from src.utility import set_business_key
from CaseGen import CaseGen, BUILTINS

@pytest.fixture
def test_data():
    """Mock for pandas.read_sql function."""
    return CaseGen().add_dict(BUILTINS)

class TestBuisnessKey:

    def test_set_business_key_ValueError(self, test_data):
        df = test_data.combine("str_normal", "int_normal").get_df()
        with pytest.raises(ValueError, match="Must provide either bk_name or table_name"):
            set_business_key(df, "str_normal", "int_normal")

    def test_set_business_key_custom_deault(self, test_data):
            df = test_data.combine("str_normal", "int_normal").get_df()
            df_result = set_business_key(df, "str_normal", "int_normal", table_name="user")
            expected = (df["str_normal"].astype(str) + "||" + df["int_normal"].astype(str))
            expected.name = "bk_user"
        
            pd.testing.assert_series_equal(df_result, expected)

    def test_set_business_key_costum_bk_name(self, test_data):
            df = test_data.combine("str_normal", "int_normal").get_df()
            df_result = set_business_key(df, "str_normal", "int_normal", bk_name="bk_cust")
            expected = (df["str_normal"].astype(str) + "||" + df["int_normal"].astype(str))
            expected.name = "bk_cust"
        
            pd.testing.assert_series_equal(df_result, expected)
            assert expected.name in df_result.name

    def test_set_business_key_mixed_col(self, test_data):
            df = test_data.combine("str_normal", "int_mixed").get_df()
            df_result = set_business_key(df, "str_normal", "int_mixed", table_name="user")
            expected = (df["str_normal"].astype(str) + "||" + df["int_mixed"].astype(str))
            expected.name = "bk_user"

            pd.testing.assert_series_equal(df_result, expected)