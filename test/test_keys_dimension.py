import pytest
from unittest.mock import patch, Mock
import pandas as pd
from sqlalchemy.engine import Connection

from src.key_dimension  import KeyDimension
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


class TestKeyDimension:

    def test_init_defaults(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_existing_dim[["bk_correct", "val_col"]]

        km = KeyDimension("correct", mock_conn, df_incoming)
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_test", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_existing_dim[["bk_test", "val_col"]]

        km = KeyDimension("correct", mock_conn, df_incoming, bk_name="bk_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_test", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )
        
        df_incoming = df_existing_dim[["bk_correct", "val_col"]]
        
        km = KeyDimension("correct", mock_conn, df_incoming, pk_name="key_test")
        
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
            KeyDimension("correct", mock_conn, df_incoming)
            
    def test_check_bk_value_no_valid_bk_values(self, test_data, mock_conn):
        "Tests existience of valid bk values"
        rename_for_test = {"str_none": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_dim_bk_none = ( test_data.combine("str_none", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_dim_bk_none[["bk_correct", "val_col"]]

        with pytest.raises(ValueError, match="No valid business key values found in column"):
            KeyDimension("correct", mock_conn, df_incoming)

    def test_check_bk_value_dublicated_values(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_dubplicated_bk_pk_values = (
             test_data.combine("str_dif", "int_dif", "str_mixed")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_dubplicated_bk_pk_values[["bk_correct", "val_col"]]
         
        with pytest.raises(ValueError, match="Duplicate business keys found in incoming data for table '"):
            KeyDimension("correct", mock_conn, df_incoming)

    @patch("pandas.read_sql")
    def test_process_default(self, mock_pd_read_sql, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )    
        df_incoming = df_existing_dim[["bk_correct", "val_col"]]
        mock_pd_read_sql.return_value = df_existing_dim[["bk_correct", "key_correct"]]

        km = KeyDimension("correct", mock_conn, df_incoming)
        df_result = km.process()

        mock_pd_read_sql.assert_called_once_with(
            "SELECT bk_correct, key_correct FROM correct",
            mock_conn
        )
        pd.testing.assert_frame_equal(df_result, df_existing_dim, check_like=True)

    @patch("pandas.read_sql")
    def test_process_called_twice(self, mock_pd_read_sql, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )  
 
        df_incoming = df_existing_dim[["bk_correct", "val_col"]]
        mock_pd_read_sql.return_value = df_existing_dim[["bk_correct", "key_correct"]]

        km = KeyDimension("correct", mock_conn, df_incoming)

        df_default_1 = km.process()
        df_default_2 = km.process()

        mock_pd_read_sql.assert_called_once_with(
            "SELECT bk_correct, key_correct FROM correct",
            mock_conn
        )
        assert km._processed == True
        pd.testing.assert_frame_equal(df_default_1, df_default_2, check_like=True)

    @patch.object(KeyDimension, "_load_existing_pairs")
    def test_assign_new_keys_default(self, mock_load_existing_dim, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_existing_dim[["bk_correct", "val_col"]]
        mock_load_existing_dim.return_value = df_existing_dim.iloc[0:3][["bk_correct", "key_correct"]]
        
        km = KeyDimension("correct", mock_conn, df_incoming)
        km.df_existing_pk_bk_pair = km._load_existing_pairs()
        km._merge_dimension_keys(km.df_existing_pk_bk_pair)
        km._assign_new_keys()

        assert km.df_incoming_modified["key_correct"].nunique() == df_existing_dim["key_correct"].nunique()
        assert km.df_incoming_modified["key_correct"].dtype.kind in ['i', 'u']

    #def test_assign_new_keys_no_new_keys(self, test_data, mock_conn):
    #    rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
    #    df_existing_dim = (
    #         test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
    #            .get_df()
    #            .rename(columns=rename_for_test)
    #    )
    #    df_incoming = df_existing_dim[["bk_correct", "val_col"]]
    #    df_existing_pk_bk_pair = df_existing_dim[["bk_correct", "key_correct"]]
    #    
    #    km = KeyDimension("correct", mock_conn, df_incoming)
    #
    #    km._merge_dimension_keys(df_existing_pk_bk_pair)
    #    km._assign_new_keys()
    #
    #    pd.testing.assert_frame_equal(km.df_incoming_modified, df_existing_dim, check_like=True)
    #
    #@patch.object(KeyDimension, "_load_existing_pairs")
    #def test_assign_new_keys_key_col_not_int(self, mock_load_existing_dim, test_data, mock_conn):
    #    rename_for_test = {"str_dif": "bk_correct", "int_dif": "val_col", "str_mixed": "key_correct"}
    #    df_existing_dim = (
    #         test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
    #            .get_df()
    #            .rename(columns=rename_for_test)
    #    )
    #
    #    df_incoming = df_existing_dim[["bk_correct", "val_col"]]
    #    df_existing_pk_bk_pair = df_existing_dim[["bk_correct", "key_correct"]]
    #    mock_load_existing_dim.return_value = df_existing_pk_bk_pair 
    #    km = KeyDimension("correct", mock_conn, df_incoming)
    #
    #    km.df_existing_pk_bk_pair =  km._load_existing_pairs()  
    #    km._merge_dimension_keys(df_existing_pk_bk_pair)
    #
    #    with pytest.raises(ValueError, match=f"Table {km.table_name} contains non-numeric PKs"):
    #        km._assign_new_keys()