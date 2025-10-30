from __future__ import annotations
from typing import Optional
import pandas as pd
from sqlalchemy.engine import Connection

BK_SEP = "||"
DEFAULT_PK_VALUE = -1
DEFAULT_PK_PREFIX = "key" #TODO
DEFAULT_BK_PREFIX = "bk" #TODO
MAX_SAMPLE_CONFLICTS = 10 #TODO
MAX_SAMPLE_ROWS = 20 #TODO

#TODO: pk er reelt surrogate nøgle
#TODO: find en bedre løsning for alle de astype kald - ligner lort og bliver gjort på data der ligger i en db 

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
        df_incoming: pd.DataFrame,
        pk_name: Optional[str] = None,
        bk_name: Optional[str] = None,
    ):
        self.table_name = table_name
        self.conn = conn
        self.df_incoming = df_incoming.copy()
        self.df_incoming_modified = df_incoming.copy()
        self.pk_name = pk_name or f"key_{table_name}"
        self.bk_name = bk_name or f"bk_{table_name}"
        self._length_incoming_df = len(df_incoming)
        self.initial_max_pk = self._get_max_existing_key()
        self._processed = False
    
    def _load_existing_pairs(self, dim_table: Optional[str] = None, pk_name: Optional[str] = None, bk_name: Optional[str] = None) -> pd.DataFrame:
        """Load existing key pairs from db."""
        bk_name = bk_name or self.bk_name  
        pk_name = pk_name or self.pk_name
        dim_table = dim_table or self.table_name
        
        query = f"SELECT {bk_name}, {pk_name} FROM {dim_table}"

        try:
            df_existing_pk_bk_pair = pd.read_sql(query, self.conn)
        except Exception as e:
            raise RuntimeError(f"Failed loading existing key pairs from {dim_table} with bk:{bk_name}, pk:{pk_name}: {e}") from e

        return df_existing_pk_bk_pair

    def _get_max_existing_key(self, table_name: Optional[str] = None, pk_name: Optional[str] = None) -> int:
        """Get maximum existing key value from database."""
        pk_name = pk_name or self.pk_name
        table_name = table_name or self.table_name
        
        query = f"SELECT COALESCE(MAX({pk_name}), 0) as max_key FROM {table_name}"
        
        try:
            result = pd.read_sql(query, self.conn)
            return int(result['max_key'].iloc[0])
        except Exception as e:
            raise RuntimeError(f"Failed getting max key from {table_name}.{pk_name}: {e}") from e

    def _assign_new_keys(self) -> None:
        "Assign new pk's for rows missing PK"
        mask_new = self.df_incoming_modified[self.pk_name].isna()
        if not mask_new.any():
            return
    
        needed = mask_new.sum()
        new_keys = range(self.initial_max_pk + 1, self.initial_max_pk + 1 + needed)
        self.df_incoming_modified.loc[mask_new, self.pk_name] = list(new_keys)
        self.df_incoming_modified[self.pk_name] = self.df_incoming_modified[self.pk_name].astype(int)
        
    def _merge_dimension_keys(self, df_existing_pk_bk_pair: pd.DataFrame, bk_name: Optional[str] = None, pk_name: Optional[str] = None) -> "KeyManager":
        """Merge dimension keys into incoming dataframe."""
        bk_name = bk_name or self.bk_name
        pk_name = pk_name or self.pk_name
        
        self.df_incoming_modified = self.df_incoming_modified.merge(
            df_existing_pk_bk_pair,
            on=bk_name,
            how="left",
        )
        
        if len(self.df_incoming_modified) > self._length_incoming_df:
            raise ValueError(f"Row count changed after merge - possible duplicate keys in {self.table_name}")
        
        return self

