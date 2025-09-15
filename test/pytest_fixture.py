import pytest
from unittest.mock import Mock, MagicMock
import pandas as pd
from sqlalchemy.engine import Connection

from TestCaseGen import TestCaseGen, BUILTINS

TestCase = TestCaseGen().add_dict(BUILTINS)

@pytest.fixture
def mock_conn():
    """Mock database connection."""
    return Mock(spec=Connection)

@pytest.fixture
def mock_existing_pairs():
    """Sample existing key pairs from database."""
    return pd.DataFrame({
        "key_table": [1, 2, 3],
        "bk_table": ["alice", "bob", "charlie"]
    })

@pytest.fixture
def incoming_data_scenarios():
    """Generate different incoming data scenarios."""
    return TestCase.combine(
        "str_case", "int_case", 
        mode="cartesian"
    ).get_df()


if __name__ == "__main__":
    print("hi")