from __future__ import annotations
from typing import Dict, Optional
import pandas as pd
from sqlalchemy.engine import Connection

from .key_manager import KeyManager
from .key_manager import (
    MAX_SAMPLE_CONFLICTS,
    MAX_SAMPLE_ROWS
)

class KeyDimension(KeyManager):
    """
    Generate new PKs for BKs not present in the dimension table.
    Writes only NEW rows (existing BKs not re-inserted).    
    Usage:
        dim = KeyDimension("dim_sales", conn, df_dim)
        dim.write_to_db()
    """

    def __init__(
        self,
        table_name: str,
        conn: Connection,
        df_incoming: pd.DataFrame,
        pk_name: Optional[str] = None,
        bk_name: Optional[str] = None,
    ):
        super().__init__(table_name, conn, df_incoming, pk_name, bk_name)
        self._check_bk_in_incoming_df()
        self._check_bk_value()

    def _check_bk_in_incoming_df(self) -> None:
        if self.bk_name not in self.df_incoming.columns:
            raise ValueError(f"Business key column '{self.bk_name}' not found in incoming dataframe")
        
    def _check_bk_value(self) -> None:
        """
        Checks BK values for:
            1. Not all BK's are None
            2. No dubplicated BK's
        """
        bk_values = self.df_incoming_modified[self.bk_name].dropna()
        
        if bk_values.empty:
            raise ValueError(f"No valid business key values found in column '{self.bk_name}'")
        
        duplicate_mask = bk_values.duplicated(keep=False)
        
        if duplicate_mask.any():
            duplicates = bk_values[duplicate_mask].drop_duplicates()
            duplicate_rows = self.df_incoming_modified[self.df_incoming_modified[self.bk_name].isin(duplicates)]
            
            raise ValueError(
                f"Duplicate business keys found in incoming data for table '{self.table_name}'. "
                f"Business key column: '{self.bk_name}'. "
                f"Duplicate values: {duplicates.tolist()[:MAX_SAMPLE_CONFLICTS]}. "
                f"Sample duplicate rows:\n{duplicate_rows.head(MAX_SAMPLE_ROWS)}"
            )
    
    def process(self) -> pd.DataFrame:
        """Process dimension data: load existing, merge, assign new keys."""
        if self._processed:
            return self.df_incoming_modified
            
        self.df_existing_pk_bk_pair = self._load_existing_pairs()
        self._merge_dimension_keys(self.df_existing_pk_bk_pair)
        self._assign_new_keys()
        
        self._processed = True 
        return self.df_incoming_modified