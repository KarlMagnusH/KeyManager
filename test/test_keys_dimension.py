import pytest
from unittest.mock import patch, Mock
import polars as pl
from sqlalchemy.engine import Connection

from keys.key_dimension import KeyDimension
from case_gen import CaseGen, BUILTINS

@pytest.fixture
def mock_conn():
    """Mock database connection."""
    return Mock(spec=Connection)

@pytest.fixture
def test_data():
    return CaseGen().add_dict(BUILTINS)


class TestKeyDimension:

    def test_init_defaults(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_normal": "val_col"}
        df_existing_dim = pl.from_pandas(
            test_data.combine("str_dif", "int_dif", "str_normal", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )
        df_incoming = df_existing_dim.select(["bk_correct", "val_col"])

        km = KeyDimension("correct", mock_conn, df_incoming)

        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_test", "int_dif": "key_correct", "str_normal": "val_col"}
        df_existing_dim = pl.from_pandas(
            test_data.combine("str_dif", "int_dif", "str_normal", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )
        df_incoming = df_existing_dim.select(["bk_test", "val_col"])

        km = KeyDimension("correct", mock_conn, df_incoming, bk_name="bk_test")

        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_test", "str_normal": "val_col"}
        df_existing_dim = pl.from_pandas(
            test_data.combine("str_dif", "int_dif", "str_normal", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )
        df_incoming = df_existing_dim.select(["bk_correct", "val_col"])

        km = KeyDimension("correct", mock_conn, df_incoming, pk_name="key_test")

        assert km.table_name == "correct"
        assert km.pk_name == "key_test"
        assert km.bk_name == "bk_correct"

    @patch.object(KeyDimension, "_get_max_existing_key")
    @patch("polars.read_database")
    def test_process_default(self, mock_read_database, mock_initial_max_pk, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_normal": "val_col"}
        df_existing_dim = pl.from_pandas(
            test_data.combine("str_dif", "int_dif", "str_normal", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )
        df_incoming = df_existing_dim.select(["bk_correct", "val_col"])
        mock_read_database.return_value = df_existing_dim.select(["bk_correct", "key_correct"])

        km = KeyDimension("correct", mock_conn, df_incoming)
        mock_initial_max_pk.return_value = df_existing_dim["key_correct"].max()

        df_result = km.process()

        mock_read_database.assert_called_once_with(
            "SELECT bk_correct, key_correct FROM correct",
            mock_conn
        )
        assert df_result.select(sorted(df_result.columns)).equals(
            df_existing_dim.select(sorted(df_existing_dim.columns))
        )

    @patch.object(KeyDimension, "_get_max_existing_key")
    @patch("polars.read_database")
    def test_process_called_twice(self, mock_read_database, mock_initial_max_pk, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_normal": "val_col"}
        df_existing_dim = pl.from_pandas(
            test_data.combine("str_dif", "int_dif", "str_normal", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )
        df_incoming = df_existing_dim.select(["bk_correct", "val_col"])
        mock_read_database.return_value = df_existing_dim.select(["bk_correct", "key_correct"])

        km = KeyDimension("correct", mock_conn, df_incoming)
        mock_initial_max_pk.return_value = df_existing_dim["key_correct"].max()

        df_default_1 = km.process()
        df_default_2 = km.process()

        mock_read_database.assert_called_once_with(
            "SELECT bk_correct, key_correct FROM correct",
            mock_conn
        )
        assert km._processed == True
        assert df_default_1.equals(df_default_2)
