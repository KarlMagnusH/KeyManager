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
    
    def process(self) -> pd.DataFrame:
        """Process dimension data: load existing, merge, assign new keys."""
        if self._processed:
            return self.df_incoming_modified
            
        self.df_existing_pk_bk_pair = self._load_existing_keys()
        self._merge_keys(self.df_existing_pk_bk_pair)
        self.initial_max_pk = self._get_max_existing_key()
        self._assign_new_keys()
        
        self._processed = True 
        return self.df_incoming_modified