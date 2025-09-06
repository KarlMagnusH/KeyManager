from __future__ import annotations
from typing import List, Sequence, Dict, Optional
import pandas as pd
from sqlalchemy.engine import Connection


BK_SEP = "||"


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
        self.pk_name = pk_name or f"key_{table_name}"
        self.bk_name = bk_name or f"bk_{table_name}"
        self._bk_cols: List[str] = []
        self._existing_pairs: pd.DataFrame | None = None
        self._prepared = False

    def set_business_key(self, columns: Sequence[str]) -> "KeyManager":
        if not columns:
            raise ValueError("Must provide at least one column for business key.")
        missing = [c for c in columns if c not in self.df_incoming.columns]
        if missing:
            raise ValueError(f"Business key source columns missing in incoming df: {missing}")
        self._bk_cols = list(columns)
        return self

    def _build_bk_column(self) -> None:
        if not self._bk_cols:
            raise ValueError("Business key components not set. Call set_business_key().")
        # Convert to string, fill NaN with sentinel to avoid accidental collisions
        tmp = self.df_incoming[self._bk_cols].astype(str).fillna("")
        self.df_incoming[self.bk_name] = tmp.agg(BK_SEP.join, axis=1)


    def _load_existing_pairs(self) -> pd.DataFrame:
        query = f"SELECT {self.pk_name}, {self.bk_name} FROM {self.table_name}"
        try:
            df = pd.read_sql(query, self.conn)
        except Exception as e:
            raise RuntimeError(f"Failed loading existing key pairs from {self.table_name}: {e}") from e
        # Normalize dtypes
        if self.bk_name in df.columns:
            df[self.bk_name] = df[self.bk_name].astype(str)
        self._existing_pairs = df
        return df


    @staticmethod
    def _assert_no_bk_conflicts(df_pairs: pd.DataFrame, bk_col: str, pk_col: str) -> None:
        if df_pairs.empty:
            return
        unique_pairs = df_pairs[[bk_col, pk_col]].dropna(subset=[bk_col, pk_col]).drop_duplicates()
        counts = unique_pairs.groupby(bk_col, as_index=False)[pk_col].nunique()
        conflicts = counts[counts[pk_col] > 1]
        if not conflicts.empty:
            sample_bks = conflicts[bk_col].tolist()[:10]
            sample_rows = unique_pairs[unique_pairs[bk_col].isin(sample_bks)]
            raise ValueError(
                f"Conflict: BKs map to multiple PKs. bk_col={bk_col}, pk_col={pk_col}, "
                f"count_conflicted={len(conflicts)}. Sample:\n{sample_rows.head(20)}"
            )


    def _left_join_existing(self) -> None:
        if self._existing_pairs is None:
            self._load_existing_pairs()
        if self._existing_pairs is None or self._existing_pairs.empty:
            self.df_incoming[self.pk_name] = pd.NA
            return
        self.df_incoming = self.df_incoming.merge(
            self._existing_pairs[[self.bk_name, self.pk_name]],
            on=self.bk_name,
            how="left",
            validate="m:1",
        )
        # Validate no (BK -> multi PK) conflict in the existing source itself #TODO: denne tjekker det allerede loadede. Hvis det kasteren fejl, er fejlen allerede sket. Det er ikke optimalt
        self._assert_no_bk_conflicts(self._existing_pairs, self.bk_name, self.pk_name)


    def prepare(self):
        """
        Common prepare steps:
          1. Build BK column
          2. Load existing (PK,BK)
          3. Join PKs onto incoming
        Dimension subclass will extend with key generation.
        """
        self._build_bk_column()
        self._load_existing_pairs()
        self._left_join_existing()
        self._prepared = True
        return self

    def result(self) -> pd.DataFrame:
        if not self._prepared:
            raise RuntimeError("Call prepare() (and persist() if dimension) before accessing result().")
        return self.df_incoming.copy()


class KeyDimension(KeyManager):
    """
    Handles surrogate key assignment for a dimension (star schema).
    May generate new PKs for BKs not present in the dimension table.
    Writes only NEW rows (existing BKs not re-inserted).
    Does not implement SCD variants beyond simple Type-1 style insertion of new keys.
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

    def _assign_new_keys(self) -> None:
        # Rows missing PK after join need new surrogate keys
        mask_new = self.df_incoming[self.pk_name].isna()
        if not mask_new.any():
            return
        # Determine next key start
        if self._existing_pairs is None or self._existing_pairs.empty:
            current_max = 0
        else:
            # Cast to numeric safely
            existing_pk = pd.to_numeric(self._existing_pairs[self.pk_name], errors="coerce")
            current_max = int(existing_pk.max()) if existing_pk.notna().any() else 0
        needed = mask_new.sum()
        new_keys = range(current_max + 1, current_max + 1 + needed)
        self.df_incoming.loc[mask_new, self.pk_name] = list(new_keys)
        self.df_incoming[self.pk_name] = self.df_incoming[self.pk_name].astype(int)

    def prepare(self):
        super().prepare()
        self._assign_new_keys()
        # Re-check no BK → multiple PK after assignment
        self._assert_no_bk_conflicts(self.df_incoming[[self.bk_name, self.pk_name]], self.bk_name, self.pk_name)
        return self

    def persist(self) -> int:
        """
        Insert only genuinely new (BK) records into the dimension table.
        Uses INSERT ... (no update). Caller should ensure uniqueness constraint on bk column in DB.
        Returns number of inserted rows.
        """
        if not self._prepared:
            raise RuntimeError("Call prepare() before persist().")
        # Identify BKs already existing
        existing_bks = set() if self._existing_pairs is None else set(self._existing_pairs[self.bk_name])
        to_insert = self.df_incoming[~self.df_incoming[self.bk_name].isin(existing_bks)].copy()
        if to_insert.empty:
            return 0
        # Only write new PK + BK (and optionally natural columns—extend as needed)
        cols_to_write = [self.pk_name, self.bk_name]
        # Write via pandas to_sql (append). Assumes table exists.
        to_insert[cols_to_write].to_sql(self.table_name, self.conn, if_exists="append", index=False)
        return len(to_insert)


class KeyFact(KeyManager):
    """
    For fact tables: only replaces BK columns (one or many) with corresponding PKs from dimension tables.
    Does NOT insert into dimension tables.
    Usage:
        fact = KeyFact("fact_sales", conn, df_fact)
        fact.register_dimension(
            dim_table="dim_product",
            fact_bk_col="bk_product",
            dim_bk_col="bk_dim_product",
            dim_pk_col="key_dim_product"
        )
        fact.prepare()  # builds its own BK if configured or skips
        fact.map_dimensions()
        df_ready = fact.result()
    """

    def __init__(
        self,
        table_name: str,
        conn: Connection,
        df_incoming: pd.DataFrame,
    ):
        super().__init__(table_name, conn, df_incoming, pk_name=None, bk_name=None)
        self._dim_mappings: List[Dict[str, str]] = []

    def register_dimension(
        self,
        dim_name: str,
        fact_bk_col: str = f"bk_{dim_name}",
        dim_bk_col: str = f"bk_{dim_name}",
        dim_pk_col: str = f"pk_{dim_name}",
        required: bool = True,
    ) -> "KeyFact":
        mapping = {
            "dim_table": dim_name,
            "fact_bk_col": fact_bk_col,
            "dim_bk_col": dim_bk_col,
            "dim_pk_col": dim_pk_col,
            "required": required,
        }
        self._dim_mappings.append(mapping)
        return self

    def register_all_dimension(self, *dim_names):
        for dim_name in dim_names:
            self.register_dimensions(dim_name)

    def prepare(self):
        # Fact may not have its own single BK like dimensions; skip parent BK logic.
        if not self._dim_mappings:
            raise RuntimeError(f"Reference til dimension is missing. Call register_all_dimension() or register_dimension() for single refereance")
        self._prepared = True
        return self

    def map_dimensions(self, fail_on_missing: bool = True):
        if not self._prepared:
            raise RuntimeError("Call prepare() first.")
        for m in self._dim_mappings:
            for col in [m["fact_bk_col"]]:
                if col not in self.df_incoming.columns:
                    raise ValueError(f"Fact BK column '{col}' missing in incoming dataframe.")
            # Load dimension slice
            query = f"SELECT {m['dim_pk_col']}, {m['dim_bk_col']} FROM {m['dim_table']}"
            dim_df = pd.read_sql(query, self.conn)
            dim_df[m["dim_bk_col"]] = dim_df[m["dim_bk_col"]].astype(str) #TODO: redundant siden det kommer fra en database?
            # Coerce fact BK col to string for reliable join
            self.df_incoming[m["fact_bk_col"]] = self.df_incoming[m["fact_bk_col"]].astype(str)
            before = len(self.df_incoming)
            self.df_incoming = self.df_incoming.merge(
                dim_df,
                left_on=m["fact_bk_col"],
                right_on=m["dim_bk_col"],
                how="left",
                validate="m:1",
            )
            assert len(self.df_incoming) == before, "Unexpected row explosion during dimension join."
            # Rename PK into fact namespace
            pk_target_name = m["dim_pk_col"]
            # Drop redundant BK from dimension side
            self.df_incoming.drop(columns=[m["dim_bk_col"]], inplace=True) #TODO: Skal rykkes ud og gøres samlet for hele facten
            # Optionally enforce all matched
            if fail_on_missing or m["required"]:
                missing = self.df_incoming[pk_target_name].isna().sum()
                if missing:
                    sample = self.df_incoming[self.df_incoming[pk_target_name].isna()][m["fact_bk_col"]].head(15)
                    raise ValueError(
                        f"Missing dimension keys for {missing} rows when mapping "
                        f"{m['fact_bk_col']} -> {pk_target_name} from {m['dim_table']}. Sample BKs:\n{sample}"
                    )
        return self