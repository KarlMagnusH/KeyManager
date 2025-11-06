#TODO: Test_import_dimension_keys
#TODO: Test process

import pytest
from unittest.mock import patch, Mock
import pandas as pd
from sqlalchemy.engine import Connection

from keys.key_fact import KeyFact
from keys.key_manager import DEFAULT_PK_VALUE
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

class TestKeyFact:

    def test_init_defaults(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        km = KeyFact("correct", mock_conn, df)
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_test", "int_dif": "key_correct", "str_mixed": "val_col"}
        df = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        km = KeyFact("correct", mock_conn, df, bk_name="bk_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_test", "str_mixed": "val_col"}
        df = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        km = KeyFact("correct", mock_conn, df, pk_name="key_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_test"
        assert km.bk_name == "bk_correct"
    
    def test_related_dimension_default(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )
        
        km = KeyFact("correct", mock_conn, df, pk_name="bk_correct")
        km.related_dimension("correct")
        
        assert km.dim_mappings["correct"]["dim_table"] == "correct"
        assert km.dim_mappings["correct"]["key_name"] == "key_correct"
        assert km.dim_mappings["correct"]["bk_name"] == "bk_correct"
    
    def test_related_dimension_modified_bk_pk(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        km = KeyFact("correct", mock_conn, df, pk_name="key_test")
        km.related_dimension("correct", bk_name="bk_test", pk_name="key_test")

        assert km.dim_mappings["correct"]["dim_table"] == "correct"
        assert km.dim_mappings["correct"]["key_name"] == "key_test"
        assert km.dim_mappings["correct"]["bk_name"] == "bk_test"

    def test_related_dimensions_with_list(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        km = KeyFact("correct", mock_conn, df)
        names = [str(i) for i in range(5)]
        km.related_dimensions(*names)

        for name in names:
            assert km.dim_mappings[name]["dim_table"] == name
            assert km.dim_mappings[name]["key_name"] == f"key_{name}"
            assert km.dim_mappings[name]["bk_name"] == f"bk_{name}"

    def test_related_dimensions_individual_args(self, test_data, mock_conn):        
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        km = KeyFact("correct", mock_conn, df)
        km.related_dimensions("users", "products", "stores")

        expected_dims = ["users", "products", "stores"]
        for name in expected_dims:
            assert km.dim_mappings[name]["dim_table"] == name
            assert km.dim_mappings[name]["key_name"] == f"key_{name}"
            assert km.dim_mappings[name]["bk_name"] == f"bk_{name}"