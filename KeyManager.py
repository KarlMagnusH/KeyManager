from __future__ import annotations
from typing import List, Sequence, Dict, Optional
import pandas as pd
from sqlalchemy.engine import Connection

BK_SEP = "||"

#TODO: pk er reelt surrogate nøgle
#TODO: find en bedre løsning for alle de astype kald - ligner lort og bliver gjort på data der ligger i en db 
#TODO: df_incoming bliver overskrevet - bør assignes til ny instansvariabel
 
def set_business_key(df_incoming: pd.DataFrame, *columns: str, table_name: str = None, bk_name: str = None):
    "Constructs and set the buinesskey"
    # TODO: Der manglere en error for hvis hverken table_name eller bk_name bliver givet
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
        self.df_incoming = df_incoming.copy() #TODO: Bør jeg lade være på med at ændre på denne og i stedet lave en ny instansvariabel, når jeg ændre på denne første gang?
        self.pk_name = pk_name or f"key_{table_name}"
        self.bk_name = bk_name or f"bk_{table_name}"
        self._length_incoming_df = len(df_incoming)
        self._prepared = False
    
    def _assert_no_bk_conflicts(self, df_pk_bk_pair: pd.DataFrame, bk_name: str, pk_name: str) -> None:
        if df_pk_bk_pair.empty:
            raise ValueError(f"The dataframe for pk-pk-pairs is empty for table {self.table_name}, bk_name={bk_name}, pk_name={pk_name}")
        unique_pairs = df_pk_bk_pair[[bk_name, pk_name]].dropna(subset=[bk_name, pk_name]).drop_duplicates()
        counts = unique_pairs.groupby(bk_name, as_index=False)[pk_name].nunique()
        conflicts = counts[counts[pk_name] > 1]
    
        if not conflicts.empty:
            sample_bks = conflicts[bk_name].tolist()[:10]
            sample_rows = unique_pairs[unique_pairs[bk_name].isin(sample_bks)]
            raise ValueError(
                f"Conflict: BKs map to multiple PKs. bk_name={bk_name}, pk_name={pk_name}, "
                f"count_conflicted={len(conflicts)}. Sample:\n{sample_rows.head(20)}"
            )
        
    def update_table_with_pk_bk_pair(self, dim_table: Optional[str] = None, pk_name: Optional[str] = None, bk_name: Optional[str] = None) -> pd.DataFrame:
        pk_name = pk_name or self.pk_name
        bk_name = bk_name or self.bk_name  
        dim_table = dim_table or self.table_name
        
        query = f"SELECT {pk_name}, {bk_name} FROM {dim_table}"
        try:
            df_pk_bk_pair = pd.read_sql(query, self.conn)
        except Exception as e:
            raise RuntimeError(f"Failed loading existing key pairs from {dim_table}: {e}") from e
        
        self.df_incoming = self.df_incoming.merge(
            df_pk_bk_pair[[bk_name, pk_name]],
            on=bk_name,
            how="left",
            validate="m:1",
        )
        if len(self.df_incoming) > self._length_incoming_df:
            raise ValueError(f"row count changed after load of existing pk's.") #TODO: potentielt lave noget inteligent show a dubletter eller nye rækker
        self._assert_no_bk_conflicts(df_pk_bk_pair, bk_name, pk_name)
        return self

    def get_modified_df(self) -> pd.DataFrame:
        return self.df_incoming.copy()

class KeyDimension(KeyManager):
    """
    Handles surrogate key assignment for a dimension (star schema).
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
        self.update_table_with_pk_bk_pair() 
        self._assert_no_bk_conflicts(self.df_pk_bk_pair, self.bk_name, self.pk_name)#TODO: Assert er opdelt. Bør samles til når de nye pk er lavet og hele dimensionen er i memory
        self._assign_new_keys()
        self._assert_no_bk_conflicts(self.df_incoming, self.bk_name, self.pk_name)

    def update_table_with_pk_bk_pair(self) -> pd.DataFrame:
        "This functions uses the instans variabels, for col names and sets a self.df_pk_bk_pair"
        query = f"SELECT {self.pk_name}, {self.bk_name} FROM {self.table_name}"
        try:
            self.df_pk_bk_pair = pd.read_sql(query, self.conn)
        except Exception as e:
            raise RuntimeError(f"Failed loading existing key pairs from {self.table_name}: {e}") from e
        
        self.df_incoming = self.df_incoming.merge(
            self.df_pk_bk_pair[[self.bk_name, self.pk_name]],
            on=self.bk_name,
            how="left",
            validate="m:1",
        )
        return self

    def _assign_new_keys(self) -> None:
        "Assign new pk's for rows missing PK after join"
        mask_new = self.df_incoming[self.pk_name].isna()
        if not mask_new.any():
            return
        # Determine next key start
        if self.df_pk_bk_pair is None or self.df_pk_bk_pair.empty:
            current_max = 0
        else:
            # Cast to numeric safely
            existing_pk = pd.to_numeric(self.df_pk_bk_pair[self.pk_name], errors="coerce")
            if existing_pk.isna().any():
                raise ValueError(f"{self.table_name} contains pk's ({self.pk_name}) that are null in the db")
            current_max = int(existing_pk.max())
        needed = mask_new.sum()
        new_keys = range(current_max + 1, current_max + 1 + needed)
        self.df_incoming.loc[mask_new, self.pk_name] = list(new_keys)
        self.df_incoming[self.pk_name] = self.df_incoming[self.pk_name].astype(int)

    #def write_to_db(self) -> int:
    #    """
    #    Insert only genuinely new (BK) records into the dimension table.
    #    Uses INSERT ... (no update). Caller should ensure uniqueness constraint on bk column in DB.
    #    """
    #    # Identify BKs already existing
    #    existing_bks = set() if self.df_pk_bk_pair is None else set(self.df_pk_bk_pair[self.bk_name])
    #    self.df_to_insert = self.df_incoming[~self.df_incoming[self.bk_name].isin(existing_bks)].copy()
    #    #if self.df_to_insert.empty:
    #    #    self.number_of_rows_inserted = 0
    #    #    print(f"") #TODO: skulle nok være en log i stedet for print
    #    # Write via pandas to_sql (append). Assumes table exists.
    #    self.df_to_insert.to_sql(self.table_name, self.conn, if_exists="append", index=False)
    #    #self.number_of_rows_inserted = len(self.df_to_insert) #TODO: Ville der være noget fint i at logge antallet afrækker der bliver skrevet?

class KeyFact(KeyManager):
    """
    For fact tables: only replaces BK columns (one or many) with corresponding PKs from dimension tables.
    Does NOT insert into dimension tables.
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

        # Fix: Direct assignment instead of .update()
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
        if not self.dim_mappings:
            raise RuntimeError("Reference til dimension is missing. Call register_all_dimension() or register_dimension() for single reference")
        
        for dim_name, m in self.dim_mappings.items():
            if m["bk_name"] not in self.df_incoming.columns:
                raise ValueError(f"Fact BK column '{m['bk_name']}' missing in incoming dataframe.")
            
            self.update_table_with_pk_bk_pair(**m)
            
            if fail_on_missing:
                missing_mask = self.df_incoming[m["pk_name"]].isna()
                missing_count = missing_mask.sum()
                if missing_count > 0:
                    sample_bks = self.df_incoming[missing_mask][m["bk_name"]].head(10)
                    raise ValueError(
                        f"Missing dimension keys for {missing_count} rows when mapping "
                        f"{m['bk_name']} -> {m['pk_name']} from {m['dim_table']}. "
                        f"Sample missing BKs:\n{sample_bks.tolist()}"
                    )
                
        bk_cols_to_pop = {m["bk_name"] for m in self.dim_mappings.values()}
        self.df_incoming = self.df_incoming.drop(columns=bk_cols_to_pop, errors='ignore')
        return self

    #def write_to_db(self) -> int:
    #    """
    #    Insert only genuinely new (BK) records into the fact table.
    #    Uses INSERT ... (no update). Caller should ensure uniqueness constraint on bk column in DB.
    #    """
    #    self.df_incoming.to_sql(self.table_name, self.conn, if_exists="append", index=False)
