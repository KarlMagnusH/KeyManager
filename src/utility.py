import pandas as pd
from typing import Optional

from .key_manager import (
    BK_SEP
)

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
    df_bk_cols_as_string = df_incoming[list(columns)].astype(str).fillna("")
    df_incoming[bk_name] = df_bk_cols_as_string.agg(BK_SEP.join, axis=1)
    return df_incoming[bk_name]
