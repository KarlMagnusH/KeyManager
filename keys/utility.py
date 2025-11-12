import pandas as pd
from typing import Optional

from .key_manager import (
    BK_SEP
)

def add_bk_for_table(for_table: str, df: pd.DataFrame, *columns: str, bk_prefix="bk") -> pd.Series:
    """
    Adds a buiness key related to a given table. The table can either be the table it self or a related table
    """

    bk_name = f"{bk_prefix}_{for_table}"
    if not columns:
        raise ValueError("Must provide at least one column for business key.")
    columns_missing = [c for c in columns if c not in df.columns]
    if columns_missing:
        raise ValueError(f"Business key source columns missing in incoming df: {columns_missing}")
    df_bk_cols_as_string = df[list(columns)].astype(str).fillna("")
    df[bk_name] = df_bk_cols_as_string.agg(BK_SEP.join, axis=1)
    return df[bk_name]
