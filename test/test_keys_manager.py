# _get_max_existing_key
#_merge_existing_

import pytest
from unittest.mock import patch, Mock
import pandas as pd
from sqlalchemy.engine import Connection

from keys.key_manager import KeyManager
from CaseGen import CaseGen, BUILTINS

@pytest.fixture
def mock_conn():
    """Mock database connection."""
    return Mock(spec=Connection)

@pytest.fixture
def mock_read_sql():
    """Mock for pandas.read_sql function."""
    return Mock()

@pytest.fixture
def test_data():
    """Mock for pandas.read_sql function."""
    return CaseGen().add_dict(BUILTINS)

class TestKeyManager:

    def test_init_defaults(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct"}

        df = test_data.combine("str_dif", "int_dif", mode="zip").get_df().rename(columns=rename_for_test)
        km = KeyManager("correct", mock_conn, df)
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_test", "int_dif": "key_correct"}

        df = test_data.combine("str_dif", "int_dif", mode="zip").get_df().rename(columns=rename_for_test)
        km = KeyManager("correct", mock_conn, df, bk_name="bk_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_test"}

        df = test_data.combine("str_dif", "int_dif", mode="zip").get_df().rename(columns=rename_for_test)
        km = KeyManager("correct", mock_conn, df, pk_name="key_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_test"
        assert km.bk_name == "bk_correct"

    def test_check_bk_in_incoming_df(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_test", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_existing_dim[["bk_test", "val_col"]]
         
        with pytest.raises(ValueError, match="Business key column 'bk_correct' not found in incoming dataframe"):
            KeyManager("correct", mock_conn, df_incoming)
            
    def test_check_bk_value_no_valid_bk_values(self, test_data, mock_conn):
        "Tests existience of valid bk values"
        rename_for_test = {"str_none": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_dim_bk_none = ( test_data.combine("str_none", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_dim_bk_none[["bk_correct", "val_col"]]

        with pytest.raises(ValueError, match="No valid business key values found in column"):
            KeyManager("correct", mock_conn, df_incoming)

    def test_check_bk_value_dublicated_values(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_dubplicated_bk_pk_values = (
             test_data.combine("str_dif", "int_dif", "str_mixed")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_dubplicated_bk_pk_values[["bk_correct", "val_col"]]
         
        with pytest.raises(ValueError, match="Duplicate business keys found in incoming data for table '"):
            KeyManager("correct", mock_conn, df_incoming)
    
    @patch('pandas.read_sql')
    def test_load_existing_keys_default(self, mock_read_sql, mock_conn, test_data):
        
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        mock_existing_pairs = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        mock_read_sql.return_value = mock_existing_pairs

        km = KeyManager("correct", mock_conn, mock_existing_pairs)
        df_result = km._load_existing_keys()
        mock_read_sql.assert_called_once_with(
            "SELECT bk_correct, key_correct FROM correct", 
            mock_conn
        )

        pd.testing.assert_frame_equal(df_result, mock_existing_pairs, check_like=True)

    def test_merge_dimension_table_keys_default(self, mock_conn, test_data):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )
        mock_df_existing_pk_bk_pair = df_existing_dim[["bk_correct", "key_correct"]]
        df_incoming = df_existing_dim[["bk_correct", "val_col"]]

        km = KeyManager("correct", mock_conn, df_incoming)
        #mock_df_existing_pk_bk_pair = km._load_existing_keys() #In reality there this happens internaly in the class
        km._merge_keys(mock_df_existing_pk_bk_pair)
        print(km.df_incoming_modified.columns)

        pd.testing.assert_frame_equal(km.df_incoming_modified, df_existing_dim, check_like=True)

    def test_assign_new_keys_no_new_keys(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )
        df_incoming = df_existing_dim[["bk_correct", "val_col"]]
        df_existing_pk_bk_pair = df_existing_dim[["bk_correct", "key_correct"]]

        km = KeyManager("correct", mock_conn, df_incoming)

        km._merge_keys(df_existing_pk_bk_pair)
        km._assign_new_keys()

        pd.testing.assert_frame_equal(km.df_incoming_modified, df_existing_dim, check_like=True)

    @patch.object(KeyManager, "_load_existing_keys")
    def test_assign_new_keys_default(self, mock_load_existing_dim, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_existing_dim[["bk_correct", "val_col"]]
        mock_existing_keys = df_existing_dim.iloc[0:3][["bk_correct", "key_correct"]]
        mock_load_existing_dim.return_value = mock_existing_keys
        km = KeyManager("correct", mock_conn, df_incoming)
        km.initial_max_pk = mock_existing_keys["key_correct"].max()
        km.df_existing_pk_bk_pair = km._load_existing_keys()
        km._merge_keys(km.df_existing_pk_bk_pair)
        km._assign_new_keys()

        assert km.df_incoming_modified["key_correct"].nunique() == df_existing_dim["key_correct"].nunique()
        assert km.df_incoming_modified["key_correct"].dtype.kind in ['i', 'u']

            
