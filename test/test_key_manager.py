import pytest
from unittest.mock import patch, Mock
import pandas as pd
from sqlalchemy.engine import Connection

from src.key_manager import KeyManager
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
        df = test_data.combine("str_normal", "int_mixed").get_df()
        km = KeyManager("correct", mock_conn, df)
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, test_data, mock_conn):
        df = test_data.combine("str_normal", "int_normal").get_df()
        km = KeyManager("correct", mock_conn, df, bk_name="bk_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, test_data, mock_conn):
        df = test_data.combine("str_normal", "int_normal").get_df()
        km = KeyManager("correct", mock_conn, df, pk_name="key_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_test"
        assert km.bk_name == "bk_correct"
    
    @patch('pandas.read_sql')
    def test_load_existing_pairs_default(self, mock_read_sql, mock_conn, test_data):
        
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        mock_existing_pairs = (
             test_data.combine("str_dif", "int_dif", "str_mixed")
                .get_df()
                .rename(columns=rename_for_test)
        )

        mock_read_sql.return_value = mock_existing_pairs

        km = KeyManager("correct", mock_conn, mock_existing_pairs)
        df_result = km._load_existing_pairs()
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
        #mock_df_existing_pk_bk_pair = km._load_existing_pairs() #In reality there this happens internaly in the class
        km.merge_dimension_keys(mock_df_existing_pk_bk_pair)
        print(km.df_incoming_modified.columns)

        pd.testing.assert_frame_equal(km.df_incoming_modified, df_existing_dim, check_like=True)