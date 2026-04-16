import pytest
from unittest.mock import patch, Mock
import polars as pl
from sqlalchemy.engine import Connection

from keys.key_manager import KeyManager
from keys.Errors import BusinessKeyError


class TestKeyManager:

    def test_init_defaults(self, dim_df, mock_conn):
        df_incoming = dim_df.select(["bk_correct", "val_col"])
        km = KeyManager("correct", mock_conn, df_incoming)

        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, mock_conn):
        df = pl.DataFrame({"bk_test": ["a", "b", "c"], "val_col": ["x", "y", "z"]})
        km = KeyManager("correct", mock_conn, df, bk_name="bk_test")

        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, dim_df, mock_conn):
        df_incoming = dim_df.select(["bk_correct", "val_col"])
        km = KeyManager("correct", mock_conn, df_incoming, pk_name="key_test")

        assert km.pk_name == "key_test"
        assert km.bk_name == "bk_correct"

    def test_check_bk_in_incoming_df(self, mock_conn):
        df = pl.DataFrame({"bk_wrong": ["a", "b"], "val_col": ["x", "y"]})
        with pytest.raises(BusinessKeyError, match="Business key column 'bk_correct' not found in incoming dataframe"):
            KeyManager("correct", mock_conn, df)

    def test_check_bk_value_no_valid_bk_values(self, mock_conn):
        df = pl.DataFrame({"bk_correct": [None, None, None], "val_col": ["x", "y", "z"]},
                          schema={"bk_correct": pl.String, "val_col": pl.String})
        with pytest.raises(BusinessKeyError, match="No valid business key values found in column"):
            KeyManager("correct", mock_conn, df)

    def test_check_bk_value_duplicated_values(self, mock_conn):
        df = pl.DataFrame({"bk_correct": ["a", "a", "b"], "val_col": ["x", "y", "z"]})
        with pytest.raises(BusinessKeyError, match="Duplicate business keys found in incoming data for table '"):
            KeyManager("correct", mock_conn, df)

    @patch("polars.read_database")
    def test_load_existing_keys_default(self, mock_read_database, dim_df, mock_conn):
        mock_read_database.return_value = dim_df.select(["bk_correct", "key_correct"])

        km = KeyManager("correct", mock_conn, dim_df.select(["bk_correct", "val_col"]))
        result = km._load_existing_keys()

        mock_read_database.assert_called_once_with(
            "SELECT bk_correct, key_correct FROM correct",
            mock_conn
        )
        assert result.equals(dim_df.select(["bk_correct", "key_correct"]))

    def test_merge_dimension_table_keys_default(self, dim_df, mock_conn):
        df_incoming = dim_df.select(["bk_correct", "val_col"])
        existing_pairs = dim_df.select(["bk_correct", "key_correct"])

        km = KeyManager("correct", mock_conn, df_incoming)
        km._merge_keys(existing_pairs)

        assert km.df_incoming_modified.select(sorted(km.df_incoming_modified.columns)).equals(
            dim_df.select(sorted(dim_df.columns))
        )

    def test_assign_new_keys_no_new_keys(self, dim_df, mock_conn):
        """All incoming rows already have keys — nothing should change."""
        df_incoming = dim_df.select(["bk_correct", "val_col"])
        existing_pairs = dim_df.select(["bk_correct", "key_correct"])

        km = KeyManager("correct", mock_conn, df_incoming)
        km.initial_max_pk = dim_df["key_correct"].max()
        km._merge_keys(existing_pairs)
        km._assign_new_keys()

        assert km.df_incoming_modified.select(sorted(km.df_incoming_modified.columns)).equals(
            dim_df.select(sorted(dim_df.columns))
        )

    @patch.object(KeyManager, "_load_existing_keys")
    def test_assign_new_keys_default(self, mock_load_existing, dim_df, mock_conn):
        """Rows not found in the DB get new sequential keys."""
        df_incoming = dim_df.select(["bk_correct", "val_col"])
        # Only the first 3 BK/key pairs exist in the DB — rows d and e are new
        existing_pairs = dim_df[:3].select(["bk_correct", "key_correct"])
        mock_load_existing.return_value = existing_pairs

        km = KeyManager("correct", mock_conn, df_incoming)
        km.initial_max_pk = existing_pairs["key_correct"].max()
        km.df_existing_pk_bk_pair = km._load_existing_keys()
        km._merge_keys(km.df_existing_pk_bk_pair)
        km._assign_new_keys()

        assert km.df_incoming_modified["key_correct"].n_unique() == dim_df["key_correct"].n_unique()
        assert km.df_incoming_modified["key_correct"].dtype == pl.Int64
