#TODO: Test _import_dimension_keys
#TODO: Test process

import pytest
from unittest.mock import Mock
import polars as pl
from sqlalchemy.engine import Connection

from keys.key_fact import KeyFact
from keys.key_manager import DEFAULT_PK_VALUE


class TestKeyFact:

    def test_init_defaults(self, dim_df, mock_conn):
        km = KeyFact("correct", mock_conn, dim_df)

        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, mock_conn):
        df = pl.DataFrame({"bk_test": ["a", "b", "c"], "val_col": ["x", "y", "z"]})
        km = KeyFact("correct", mock_conn, df, bk_name="bk_test")

        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, dim_df, mock_conn):
        km = KeyFact("correct", mock_conn, dim_df, pk_name="key_test")

        assert km.pk_name == "key_test"
        assert km.bk_name == "bk_correct"

    def test_related_dimension_default(self, dim_df, mock_conn):
        km = KeyFact("correct", mock_conn, dim_df, pk_name="bk_correct")
        km.related_dimension("correct")

        assert km.dim_mappings["correct"]["dim_table"] == "correct"
        assert km.dim_mappings["correct"]["key_name"] == "key_correct"
        assert km.dim_mappings["correct"]["bk_name"] == "bk_correct"

    def test_related_dimension_modified_bk_pk(self, dim_df, mock_conn):
        km = KeyFact("correct", mock_conn, dim_df, pk_name="key_correct")
        km.related_dimension("correct", bk_name="bk_test", pk_name="key_test")

        assert km.dim_mappings["correct"]["dim_table"] == "correct"
        assert km.dim_mappings["correct"]["key_name"] == "key_test"
        assert km.dim_mappings["correct"]["bk_name"] == "bk_test"

    def test_related_dimensions_with_list(self, dim_df, mock_conn):
        km = KeyFact("correct", mock_conn, dim_df)
        names = [str(i) for i in range(5)]
        km.related_dimensions(*names)

        for name in names:
            assert km.dim_mappings[name]["dim_table"] == name
            assert km.dim_mappings[name]["key_name"] == f"key_{name}"
            assert km.dim_mappings[name]["bk_name"] == f"bk_{name}"

    def test_related_dimensions_individual_args(self, dim_df, mock_conn):
        km = KeyFact("correct", mock_conn, dim_df)
        km.related_dimensions("users", "products", "stores")

        for name in ["users", "products", "stores"]:
            assert km.dim_mappings[name]["dim_table"] == name
            assert km.dim_mappings[name]["key_name"] == f"key_{name}"
            assert km.dim_mappings[name]["bk_name"] == f"bk_{name}"
