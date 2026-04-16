import pytest
from unittest.mock import Mock
import polars as pl
from sqlalchemy.engine import Connection


@pytest.fixture
def mock_conn():
    return Mock(spec=Connection)


@pytest.fixture
def dim_df():
    """Five-row dimension table: unique BKs, sequential PKs, a value column."""
    return pl.DataFrame({
        "bk_correct": ["a", "b", "c", "d", "e"],
        "key_correct": [1, 2, 3, 4, 5],
        "val_col":     ["v1", "v2", "v3", "v4", "v5"],
    })
