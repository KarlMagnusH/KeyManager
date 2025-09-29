from __future__ import annotations
from typing import List, Sequence, Dict, Optional
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
 
def set_business_key(df_incoming: pd.DataFrame, *columns: str, table_name: Optional[str] = None, bk_name: Optional[str] = None):
    if bk_name is None and table_name is None:
        raise ValueError("Must provide either bk_name or table_name")
    if bk_name is None:
        bk_name = f"bk_{table_name}"

    if not columns:
        raise ValueError("Must provide at least one column for business key.")
    columns_missing = [c for c in columns if c not in df_incoming.columns]
    if columns_missing:
        raise ValueError(f"Business key source columns missing in incoming df: {columns_missing}")
    df_bk_cols_as_string = df_incoming[columns].astype(str).fillna("")
    df_incoming[bk_name] = df_bk_cols_as_string.agg(BK_SEP.join, axis=1)
    return df_incoming[bk_name] #TODO: Bør denne retunere en df eller kolonne? (find ud af når den bruges)

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
        self._processed = False
    
    def _load_existing_pairs(self, dim_table: Optional[str] = None, pk_name: Optional[str] = None, bk_name: Optional[str] = None) -> pd.DataFrame:
        """Load existing key pairs from dimension table."""
        bk_name = bk_name or self.bk_name  
        pk_name = pk_name or self.pk_name
        dim_table = dim_table or self.table_name
        
        query = f"SELECT {pk_name}, {bk_name} FROM {dim_table}"

        try:
            df_pk_bk_pair = pd.read_sql(query, self.conn)
        except Exception as e:
            raise RuntimeError(f"Failed loading existing key pairs from {dim_table} with bk:{bk_name}, pk:{pk_name}: {e}") from e

        return df_pk_bk_pair
        
    def merge_dimension_keys(self, df_pk_bk_pair: pd.DataFrame, bk_name: Optional[str] = None, pk_name: Optional[str] = None) -> "KeyManager":
        """Merge dimension keys into incoming dataframe."""
        bk_name = bk_name or self.bk_name
        pk_name = pk_name or self.pk_name
        
        self.df_incoming_modified = self.df_incoming_modified.merge(
            df_pk_bk_pair,
            on=bk_name,
            how="left",
        )
        
        if len(self.df_incoming_modified) > self._length_incoming_df:
            raise ValueError(f"Row count changed after merge - possible duplicate keys in {self.table_name}")
        
        return self

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

    def __call__(self) -> pd.DataFrame:
        """Process dimension data: validate, load existing, merge, assign new keys."""
        if self._processed:
            return self.df_incoming_modified
            
        self.df_pk_bk_pair = self._load_existing_pairs()
        self.merge_dimension_keys(self.df_pk_bk_pair)
        self._assign_new_keys()
        
        self._processed = True

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

    def _assign_new_keys(self) -> None:
        "Assign new pk's for rows missing PK after join"
        mask_new = self.df_incoming_modified[self.pk_name].isna()
        if not mask_new.any():
            return

        current_max = 0
        if not self.df_pk_bk_pair.empty:
            existing_pk = pd.to_numeric(self.df_pk_bk_pair[self.pk_name], errors="coerce") 
            if existing_pk.isna().any():
                raise ValueError(f"Table {self.table_name} contains non-numeric PKs")
            current_max = int(existing_pk.max())
    
        needed = mask_new.sum()
        new_keys = range(current_max + 1, current_max + 1 + needed)
        self.df_incoming_modified.loc[mask_new, self.pk_name] = list(new_keys)
        self.df_incoming_modified[self.pk_name] = self.df_incoming_modified[self.pk_name].astype(int)
    
    def process(self) -> pd.DataFrame:
        """Alias for __call__ if you prefer explicit method name."""
        self()      
        return self.df_incoming_modified

class KeyFact(KeyManager):
    """
    For fact tables: only replaces BK columns (one or many) with corresponding PKs from dimension tables.
    Usage:
        fact = KeyFact("fact_sales", conn, df_fact)
        fact.register_dimension(
            dim_table="dim_table",
            pk_name="pk_name",
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

    def register_dimension(
        self,
        dim_name: str,
        bk_name: Optional[str] = None,
        pk_name: Optional[str] = None,
    ) -> "KeyFact":
        
        bk_name = bk_name or f"bk_{dim_name}" 
        pk_name = pk_name or f"pk_{dim_name}"

        self.dim_mappings[dim_name] = {
            "dim_table": dim_name,
            "bk_name": bk_name,
            "pk_name": pk_name,
        }
        return self

    def register_all_dimensions(self, *dim_names):
        for dim_name in dim_names:
            self.register_dimension(dim_name=dim_name)
        return self
    
    def import_dimension_keys(self, fail_on_missing: bool = True):
        if self._processed == True:
            return self
        if not self.dim_mappings:
            raise RuntimeError("Reference til dimension is missing...")
        
        for dim_name, m in self.dim_mappings.items():
            if m["bk_name"] not in self.df_incoming_modified.columns:
                raise ValueError(f"Fact BK column '{m['bk_name']}' missing in incoming dataframe.")
            
            df_pairs = self._load_existing_pairs(
                dim_table=m["dim_table"],
                pk_name=m["pk_name"], 
                bk_name=m["bk_name"]
            )
            self.merge_dimension_keys(df_pairs, m["bk_name"], m["pk_name"])
            
            missing_mask = self.df_incoming_modified[m["pk_name"]].isna()
            missing_count = missing_mask.sum()
            if fail_on_missing and missing_count > 0:
                sample_bks = self.df_incoming_modified[missing_mask][m["bk_name"]].head(10)
                raise ValueError(
                    f"Missing dimension keys for {missing_count} rows when mapping "
                    f"{m['bk_name']} -> {m['pk_name']} from {m['dim_table']}. "
                    f"Sample missing BKs:\n{sample_bks.tolist()}"
                    )
            else:
                self.df_incoming_modified.loc[missing_mask, m['pk_name']] = DEFAULT_PK_VALUE
                
        bk_cols_to_pop = {m["bk_name"] for m in self.dim_mappings.values()}
        self.df_incoming_modified = self.df_incoming_modified.drop(columns=bk_cols_to_pop, errors='ignore')
        self._processed = True
        return self

    def process(self) -> pd.DataFrame:
        self.import_dimension_keys()
        return self.df_incoming_modified
    #def write_to_db(self) -> int:
    #    """
    #    Insert only genuinely new (BK) records into the fact table.
    #    Uses INSERT ... (no update). Caller should ensure uniqueness constraint on bk column in DB.
    #    """
    #    self.df_incoming.to_sql(self.table_name, self.conn, if_exists="append", index=False)
