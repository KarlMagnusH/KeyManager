from __future__ import annotations
from typing import Optional
import polars as pl
from sqlalchemy.engine import Connection
from .Errors import BusinessKeyError, DatabaseError, MergeError

BK_SEP = "||"
DEFAULT_PK_VALUE = -1
DEFAULT_PK_PREFIX = "key" #TODO
DEFAULT_BK_PREFIX = "bk" #TODO
MAX_SAMPLE_CONFLICTS = 5 #TODO
MAX_SAMPLE_ROWS = 5 #TODO

#TODO: pk er reelt surrogate nøgle

class KeyManager:
    """
    Base class for key handling.
    Holds the incoming dataframe and provides:
      - Business key construction
      - Conflict checking (BK -> PK uniqueness)
    Subclasses decide whether they may generate new PKs (dimension) or only look up (fact).
    """

    def __init__(
        self,
        table_name: str,
        conn: Connection,
        df_incoming: pl.DataFrame,
        pk_name: Optional[str] = None,
        bk_name: Optional[str] = None,
        key_condition: Optional[str] = None,
    ):
        self.table_name = table_name
        self.conn = conn
        self.df_incoming = df_incoming.clone()
        self.df_incoming_modified = df_incoming.clone()
        self.pk_name = pk_name or f"key_{table_name}"
        self.bk_name = bk_name or f"bk_{table_name}"
        self.key_condition = key_condition
        self._initial_length_incoming_df = len(df_incoming)
        self._check_bk_in_incoming_df()
        self._check_bk_value()
        self._processed = False

    def _check_bk_in_incoming_df(self) -> None:
        if self.bk_name not in self.df_incoming.columns:
            raise BusinessKeyError(f"Business key column '{self.bk_name}' not found in incoming dataframe")

    def _check_bk_value(self) -> None:
        """
        Checks BK values for:
            1. Not all BK's are None
            2. No duplicated BK's
        """
        bk_values = self.df_incoming_modified[self.bk_name].drop_nulls()

        if len(bk_values) == 0:
            raise BusinessKeyError(f"No valid business key values found in column '{self.bk_name}'")

        duplicate_mask = bk_values.is_duplicated()

        if duplicate_mask.any():
            duplicates = bk_values.filter(duplicate_mask).unique()
            example_dub = duplicates[0]
            duplicate_rows = self.df_incoming_modified.filter(pl.col(self.bk_name) == example_dub)

            raise BusinessKeyError(
                f"Duplicate business keys found in incoming data for table '{self.table_name}'. "
                f"Business key column: '{self.bk_name}'. "
                f"Duplicate values: {duplicates}."
                f"Sample duplicate rows:\n{duplicate_rows}"
            )

    def _load_existing_keys(self, dim_table: Optional[str] = None, pk_name: Optional[str] = None, bk_name: Optional[str] = None) -> pl.DataFrame:
        """Load existing key pairs from db."""
        bk_name = bk_name or self.bk_name
        pk_name = pk_name or self.pk_name
        dim_table = dim_table or self.table_name

        query = f"SELECT {bk_name}, {pk_name} FROM {dim_table}"
        if self.key_condition:
            query += " WHERE " + self.key_condition

        try:
            df_existing_pk_bk_pair = pl.read_database(query, self.conn)
        except Exception as e:
            raise DatabaseError(f"Failed loading existing key pairs from {dim_table} with bk:{bk_name}, pk:{pk_name}: {e}") from e

        return df_existing_pk_bk_pair

    def _get_max_existing_key(self, table_name: Optional[str] = None, pk_name: Optional[str] = None) -> int:
        """Get maximum existing key value from database."""
        pk_name = pk_name or self.pk_name
        table_name = table_name or self.table_name

        query = f"SELECT COALESCE(MAX({pk_name}), 0) as max_key FROM {table_name}"

        try:
            result = pl.read_database(query, self.conn)
            return int(result['max_key'][0])
        except Exception as e:
            raise DatabaseError(f"Failed getting max key from {table_name}.{pk_name}: {e}") from e

    def _assign_new_keys(self) -> None:
        "Assign new pk's for rows missing PK"
        mask_new = self.df_incoming_modified[self.pk_name].is_null()
        if not mask_new.any():
            return

        self.df_incoming_modified = self.df_incoming_modified.with_columns(
            pl.when(pl.col(self.pk_name).is_null())
            .then(self.initial_max_pk + pl.col(self.pk_name).is_null().cast(pl.Int64).cum_sum())
            .otherwise(pl.col(self.pk_name).cast(pl.Int64))
            .alias(self.pk_name)
        )

    def _merge_keys(self, df_existing_pk_bk_pair: pl.DataFrame, bk_name: Optional[str] = None, pk_name: Optional[str] = None) -> "KeyManager":
        """Merge dimension keys into incoming dataframe."""
        bk_name = bk_name or self.bk_name
        pk_name = pk_name or self.pk_name

        self.df_incoming_modified = self.df_incoming_modified.join(
            df_existing_pk_bk_pair,
            on=bk_name,
            how="left",
        )

        if len(self.df_incoming_modified) > self._initial_length_incoming_df:
            raise MergeError(f"Row count changed after merge - possible duplicate keys in {self.table_name}")

        return self
