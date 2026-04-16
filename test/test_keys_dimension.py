import pytest
from unittest.mock import patch, Mock
import polars as pl
from sqlalchemy.engine import Connection

from keys.key_dimension import KeyDimension


class TestKeyDimension:

    def test_init_defaults(self, dim_df, mock_conn):
        km = KeyDimension("correct", mock_conn, dim_df.select(["bk_correct", "val_col"]))

        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, mock_conn):
        df = pl.DataFrame({"bk_test": ["a", "b", "c"], "val_col": ["x", "y", "z"]})
        km = KeyDimension("correct", mock_conn, df, bk_name="bk_test")

        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, dim_df, mock_conn):
        km = KeyDimension("correct", mock_conn, dim_df.select(["bk_correct", "val_col"]), pk_name="key_test")

        assert km.pk_name == "key_test"
        assert km.bk_name == "bk_correct"

    @patch.object(KeyDimension, "_get_max_existing_key")
    @patch("polars.read_database")
    def test_process_default(self, mock_read_database, mock_get_max, dim_df, mock_conn):
        """All incoming BKs exist in the DB — keys are looked up, none assigned."""
        df_incoming = dim_df.select(["bk_correct", "val_col"])
        mock_read_database.return_value = dim_df.select(["bk_correct", "key_correct"])
        mock_get_max.return_value = dim_df["key_correct"].max()

        km = KeyDimension("correct", mock_conn, df_incoming)
        df_result = km.process()

        mock_read_database.assert_called_once_with(
            "SELECT bk_correct, key_correct FROM correct",
            mock_conn
        )
        assert df_result.select(sorted(df_result.columns)).equals(
            dim_df.select(sorted(dim_df.columns))
        )

    @patch.object(KeyDimension, "_get_max_existing_key")
    @patch("polars.read_database")
    def test_process_called_twice(self, mock_read_database, mock_get_max, dim_df, mock_conn):
        """Calling process() twice returns the same result and only reads the DB once."""
        df_incoming = dim_df.select(["bk_correct", "val_col"])
        mock_read_database.return_value = dim_df.select(["bk_correct", "key_correct"])
        mock_get_max.return_value = dim_df["key_correct"].max()

        km = KeyDimension("correct", mock_conn, df_incoming)
        result_1 = km.process()
        result_2 = km.process()

        mock_read_database.assert_called_once_with(
            "SELECT bk_correct, key_correct FROM correct",
            mock_conn
        )
        assert km._processed is True
        assert result_1.equals(result_2)
