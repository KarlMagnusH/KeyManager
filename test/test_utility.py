import polars as pl

from keys.utility import add_bk_for_table


class TestBusinessKey:

    def test_add_bk_for_table_col_exists(self):
        """The BK column is added to the DataFrame."""
        df = pl.DataFrame({"str_col": ["a", "b", "c"], "int_col": [1, 2, 3]})
        result = df.with_columns(add_bk_for_table("correct", df, "str_col", "int_col"))
        assert "bk_correct" in result.columns

    def test_add_bk_for_table_values(self):
        """BK values are the source columns cast to string and joined with the separator."""
        df = pl.DataFrame({"str_col": ["a", "b", "c"], "int_col": [1, 2, 3]})
        result = df.with_columns(add_bk_for_table("correct", df, "str_col", "int_col"))

        expected = (df["str_col"].cast(pl.String) + "||" + df["int_col"].cast(pl.String)).alias("bk_correct")
        assert result["bk_correct"].equals(df.select(expected).to_series())
