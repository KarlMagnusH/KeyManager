import pytest
from unittest.mock import patch, Mock
import pandas as pd
from sqlalchemy.engine import Connection

from KeyManager import KeyManager, KeyDimension, KeyFact, set_business_key
from TestCaseGen import TestCaseGen, BUILTINS

test_data = TestCaseGen().add_dict(BUILTINS)

@pytest.fixture
def mock_conn():
    """Mock database connection."""
    return Mock(spec=Connection)

@pytest.fixture
def mock_read_sql():
    """Mock for pandas.read_sql function."""
    return Mock()

@pytest.fixture
def df():
    """Create test DataFrame from TestCaseGen data."""
    return test_data.combine("str_normal", "int_normal").get_df()

@pytest.fixture
def df_int_mixed():
    """Create test DataFrame from TestCaseGen data."""
    return test_data.combine("str_normal", "int_mixed").get_df()

@pytest.fixture
def df_3_cols():
    """Create test DataFrame from TestCaseGen data."""
    return test_data.combine("str_normal", "int_same", "str_mixed").get_df()

@pytest.fixture
def df_3_cols_zipped():
    """Create test DataFrame from TestCaseGen data."""
    return test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip").get_df()

@pytest.fixture
def df_3_cols_str_none_zipped():
    """Create test DataFrame from TestCaseGen data."""
    return test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip").get_df()

class TestBuisnessKey:

    def test_set_business_key_ValueError(self, df):
        with pytest.raises(ValueError, match="Must provide either bk_name or table_name"):
            set_business_key(df, "str_normal", "int_normal")

    def test_set_business_key_custom_bk_name(self, df):
            df_result = set_business_key(df, "str_normal", "int_normal", table_name="user")
            expected = (df["str_normal"].astype(str) + "||" + df["int_normal"].astype(str))
            expected.name = "bk_user"
        
            pd.testing.assert_series_equal(df_result, expected)

    def test_set_business_key_costum_bk_name(self, df):
            df_result = set_business_key(df, "str_normal", "int_normal", bk_name="bk_cust")
            expected = (df["str_normal"].astype(str) + "||" + df["int_normal"].astype(str))
            expected.name = "bk_cust"
        
            pd.testing.assert_series_equal(df_result, expected)
            assert expected.name in df_result.name


    def test_set_business_key_mixed_col(self, df_int_mixed):
            df_result = set_business_key(df_int_mixed, "str_normal", "int_mixed", table_name="user")
            expected = (df_int_mixed["str_normal"].astype(str) + "||" + df_int_mixed["int_mixed"].astype(str))
            expected.name = "bk_cust"

            pd.testing.assert_series_equal(df_result, expected)
            assert expected.name in df_result.name

class TestKeyManager:

    def test_init_defaults(self, df, mock_conn):
        km = KeyManager("correct", mock_conn, df)
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, df, mock_conn):
        km = KeyManager("correct", mock_conn, df, bk_name="bk_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, df, mock_conn):
        km = KeyManager("correct", mock_conn, df, pk_name="key_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_test"
        assert km.bk_name == "bk_correct"
    
    @patch('pandas.read_sql')
    def test_load_existing_pairs_default(self, mock_read_sql, mock_conn, df):
        rename_for_test = {"str_normal": "bk_correct", "int_normal": "key_correct"}

        mock_existing_pairs = df.rename(columns=rename_for_test)
        mock_read_sql.return_value = mock_existing_pairs
        
        df_result = KeyManager("correct", mock_conn, df)._load_existing_pairs()
        
        mock_read_sql.assert_called_once_with(
            "SELECT key_correct, bk_correct FROM correct", 
            mock_conn
        )
        pd.testing.assert_frame_equal(df_result, mock_existing_pairs)

    def test_merge_dimension_table_keys_default(self, df_3_cols, mock_conn):

        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}

        mock_df_pk_bk_pair = df_3_cols.rename(columns=rename_for_test)

        km = KeyManager("correct", mock_conn, mock_df_pk_bk_pair.loc[:, ["bk_correct", "val_col"]])
        #mock_df_pk_bk_pair = km._load_existing_pairs() #In reality there this happens internaly in the class
        km.merge_dimension_keys(mock_df_pk_bk_pair)

        pd.testing.assert_frame_equal(
            km.df_incoming_modified, 
            mock_df_pk_bk_pair.loc[:, ["bk_correct", "key_correct", "val_col"]]
            )

class TestKeyDimension:

    def test_init_defaults(self, df_3_cols_zipped, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_incoming = df_3_cols_zipped.rename(columns=rename_for_test)
        km = KeyDimension("correct", mock_conn, df_incoming)
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, df_3_cols_zipped, mock_conn):
        rename_for_test = {"str_normal": "bk_test", "int_normal": "key_correct", "str_mixed": "val_col"}
        df_incoming = df_3_cols_zipped.rename(columns=rename_for_test)
        km = KeyDimension("correct", mock_conn, df_incoming, bk_name="bk_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, df_3_cols_zipped, mock_conn):
        rename_for_test = {"str_normal": "bk_correct", "int_normal": "key_test", "str_mixed": "val_col"}
        df_incoming = df_3_cols_zipped.rename(columns=rename_for_test)
        km = KeyDimension("correct", mock_conn, df_incoming, pk_name="key_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_test"
        assert km.bk_name == "bk_correct"

    def test_check_bk_in_incoming_df_dub(self, df_3_cols, mock_conn):

        rename_for_test = {"str_normal": "bk_test", "int_normal": "key_correct", "str_mixed": "val_col"}
        df_incoming = df_3_cols.rename(rename_for_test)
         
        with pytest.raises(ValueError, match="Business key column bk_correct not found in incoming dataframe"):
            KeyDimension("correct", mock_conn, df_incoming)
    
    def test_check_bk_value_no_valid_bk_values(self, df_3_cols_str_none_zipped, mock_conn):
        "tests valid bk values (some non none)"
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_incoming = df_3_cols_str_none_zipped.rename(rename_for_test)
         
        with pytest.raises(ValueError, match="No valid business key values found in column"):
            KeyDimension("correct", mock_conn, df_incoming)

    def test_check_bk_value_dublicated_values(self, df_3_cols, mock_conn):
        "tests dublicated values in bk"
        rename_for_test = {"str_normal": "bk_test", "int_normal": "key_correct", "str_mixed": "val_col"}
        df_incoming = df_3_cols.rename(rename_for_test)
         
        with pytest.raises(ValueError, match="Duplicate business keys found in incoming data for table '"):
            KeyDimension("correct", mock_conn, df_incoming)

    def test_assign_new_keys_default(self, df_3_cols_zipped, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_incoming = df_3_cols_zipped.rename(columns=rename_for_test)

        df_incoming["key_correct"] = 0
        km = KeyDimension("correct", mock_conn, df_incoming)
        km.df_pk_bk_pair = df_incoming.iloc[0:3][["bk_correct", "key_correct"]]

        km._assign_new_keys()

        pd.testing.assert_frame_equal(km.df_incoming_modified, df_3_cols_zipped)

    def test_assign_new_keys_no_new_keys(self, df_3_cols_zipped, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_incoming = df_3_cols_zipped.rename(columns=rename_for_test)
        
        km = KeyDimension("correct", mock_conn, df_incoming)
        km.df_pk_bk_pair = df_incoming.loc[["bk_correct", "key_correct"]]
        km._assign_new_keys()

        pd.testing.assert_frame_equal(km.df_incoming_modified, df_3_cols_zipped)

    def test_assign_new_keys_key_col_not_int(self, df_3_cols_zipped, mock_conn):
        rename_for_test = {"str_normal": "bk_correct", "int_normal": "val_col", "str_mixed": "key_correct"}
        df_incoming = df_3_cols_zipped.rename(columns=rename_for_test)
        
        km = KeyDimension("correct", mock_conn, df_incoming)
        km.df_pk_bk_pair = df_incoming.loc[["bk_correct", "key_correct"]]
        with pytest.raises(ValueError, match=f"Table {km.table_name} contains non-numeric PKs"):
            km._assign_new_keys()

    def test__call__default(self, df_3_cols_zipped, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_incoming = df_3_cols_zipped.rename(columns=rename_for_test)

        df_incoming["key_correct"] = 0
        km = KeyDimension("correct", mock_conn, df_incoming)
        km.df_pk_bk_pair = df_incoming.iloc[0:3][["bk_correct", "key_correct"]]

        df_default = km()

        pd.testing.assert_frame_equal(df_default, df_3_cols_zipped)

    def test_process_default(self, df_3_cols_zipped, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_incoming = df_3_cols_zipped.rename(columns=rename_for_test)

        df_incoming["key_correct"] = 0
        km = KeyDimension("correct", mock_conn, df_incoming)
        km.df_pk_bk_pair = df_incoming.iloc[0:3][["bk_correct", "key_correct"]]

        df_default = km.process()
    
        pd.testing.assert_frame_equal(df_default, df_3_cols_zipped)

    def test_process_called_twice(self, df_3_cols_zipped, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_incoming = df_3_cols_zipped.rename(columns=rename_for_test)

        df_incoming["key_correct"] = 0
        km = KeyDimension("correct", mock_conn, df_incoming)
        km.df_pk_bk_pair = df_incoming.iloc[0:3][["bk_correct", "key_correct"]]

        df_default_1 = km.process()
        df_default_2 = km.process()
    
        pd.testing.assert_frame_equal(df_default_1, df_default_2)
        pd.testing.assert_frame_equal(df_default_2, df_3_cols_zipped)

        
class TestKeyFact:
    pass