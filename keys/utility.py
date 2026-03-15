import polars as pl

from .key_manager import BK_SEP

def add_bk_for_table(for_table: str, df: pl.DataFrame, *columns: str, bk_prefix="bk") -> pl.Expr:
    """
    Adds a business key related to a given table. The table can either be the table itself or a related table.
    Returns a Polars expression; use with df.with_columns(add_bk_for_table(...)).
    """
    bk_name = f"{bk_prefix}_{for_table}"
    if not columns:
        raise ValueError("Must provide at least one column for business key.")
    if columns_missing := [c for c in columns if c not in df.columns]:
        raise ValueError(f"Business key source columns missing in incoming df: {columns_missing} \n columns available: {df.columns}")
    return pl.concat_str(
        [pl.col(c).cast(pl.String).fill_null("") for c in columns],
        separator=BK_SEP,
    ).alias(bk_name)
