from __future__ import annotations
from typing import Dict, Optional
import pandas as pd
from sqlalchemy.engine import Connection

from .key_manager import KeyManager
from .key_manager import (
    DEFAULT_PK_VALUE,
)

class KeyFact(KeyManager):
    """
    For fact tables: only replaces BK columns (one or many) with corresponding PKs from dimension tables.
    Usage:
        fact = KeyFact("fact_sales", conn, df_fact)
        fact.register_dimension(
            dim_table="dim_table",
            pk_name="key_name",
            bk_name="bk_name"
        )
        fact.import_dimension_keys()
        fact.write_to_db()
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
        self.dim_mappings: Dict[str, Dict[str, str]] = {}
    
    def _import_dimension_keys(self, fail_on_missing: bool = False):
        #TODO: This function should be split in multiple
        if self._processed == True:
            return self
        if not self.dim_mappings:
            raise RuntimeError("Reference to dimension is missing. Either register_dimension or register_all_dimension must be called.")
        
        for dim_name, m in self.dim_mappings.items():
            if m["bk_name"] not in self.df_incoming_modified.columns:
                raise ValueError(f"Fact BK column '{m['bk_name']}' missing in incoming dataframe.")
            
            df_pairs = self._load_existing_pairs(
                dim_table=m["dim_table"],
                pk_name=m["key_name"], 
                bk_name=m["bk_name"]
            )
            self._merge_dimension_keys(df_pairs, m["bk_name"], m["key_name"])
            
            missing_mask = self.df_incoming_modified[m["key_name"]].isna()
            missing_count = missing_mask.sum()
            if fail_on_missing and missing_count > 0:
                sample_bks = self.df_incoming_modified[missing_mask][m["bk_name"]].head(10)
                raise ValueError(
                    f"Missing dimension keys for {missing_count} rows when mapping "
                    f"{m['bk_name']} -> {m['key_name']} from {m['dim_table']}. "
                    f"Sample missing BKs:\n{sample_bks.tolist()}"
                    )
            else:
                self.df_incoming_modified.loc[missing_mask, m["key_name"]] = DEFAULT_PK_VALUE
                
        bk_cols_to_pop = {m["bk_name"] for m in self.dim_mappings.values()}
        self.df_incoming_modified = self.df_incoming_modified.drop(columns=bk_cols_to_pop, errors="ignore")
        self._processed = True
        return self

    def related_dimension(
        self,
        dim_name: str,
        bk_name: Optional[str] = None,
        pk_name: Optional[str] = None,
    ) -> "KeyFact":
        
        bk_name = bk_name or f"bk_{dim_name}" 
        pk_name = pk_name or f"key_{dim_name}"

        self.dim_mappings[dim_name] = {
            "dim_table": dim_name,
            "bk_name": bk_name,
            "key_name": pk_name,
        }
        return self

    def related_dimensions(self, *related_dimensions: str):
        for dim_name in related_dimensions:
            self.related_dimension(dim_name=dim_name)
        return self

    def process(self) -> pd.DataFrame:
        self._import_dimension_keys()
        self._assign_new_keys()
        return self.df_incoming_modified
