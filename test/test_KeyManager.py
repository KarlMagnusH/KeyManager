import pytest
from unittest.mock import patch, Mock
import pandas as pd
from sqlalchemy.engine import Connection

from KeyManager import KeyManager, KeyDimension
from TestCaseGen import TestCaseGen, BUILTINS

test_data = TestCaseGen().add_dict(BUILTINS)

@pytest.fixture
def mock_conn():
    """Mock database connection."""
    return Mock(spec=Connection)

@pytest.fixture
def mock_read_sql():
    """Mock for pandas.read_sql function."""
    return Mock()

@pytest.fixture
def mock_existing_pairs():
    """Sample existing key pairs from database."""
    return pd.DataFrame({
        "key_users": [1, 2, 3],
        "bk_users": ["alice", "bob", "charlie"]
    })


class TestKeyManager:
    
    def test_init_defaults(self, mock_conn):
        """Test initialization with default names."""
        df = pd.DataFrame({"name": ["alice"]})
        km = KeyManager("users", mock_conn, df)
        
        assert km.table_name == "users"
        assert km.pk_name == "key_users"
        assert km.bk_name == "bk_users"
    
    def test_set_business_key_valid(self, mock_conn):
        """Test setting valid business key columns."""
        df = pd.DataFrame({"name": ["alice"], "age": [25]})
        km = KeyManager("users", mock_conn, df)
        
        result = km.set_business_key(["name", "age"])
        assert km._bk_cols == ["name", "age"]
        assert result is km  # fluent interface
    
    def test_set_business_key_missing_columns(self, mock_conn):
        """Test error when BK columns missing from dataframe."""
        df = pd.DataFrame({"name": ["alice"]})
        km = KeyManager("users", mock_conn, df)
        
        with pytest.raises(ValueError, match="missing in incoming df"):
            km.set_business_key(["name", "missing_col"])
    

    @patch('pandas.read_sql')
    def test_load_existing_pairs(self, mock_read_sql, mock_conn, mock_existing_pairs):
        """Test loading existing key pairs from database."""
        df = pd.DataFrame({"name": ["alice"]})
        km = KeyManager("users", mock_conn, df)
                
        with patch('pandas.read_sql', return_value=mock_existing_pairs) as mock_read_sql:
            result = km._load_existing_pairs()
            
            mock_read_sql.assert_called_once_with(
                "SELECT key_users, bk_users FROM users", 
                mock_conn
            )
            pd.testing.assert_frame_equal(result, mock_existing_pairs)
    
    @patch('pandas.read_sql')
    def test_load_existing_pairs_missing_table(self, mock_read_sql, mock_conn):
        """Test error when database table doesn't exist."""
        mock_read_sql.side_effect = KeyError("Table 'users' doesn't exist")
        df = pd.DataFrame({"name": ["alice"]})
        km = KeyManager("users", mock_conn, df)
        
        with pytest.raises(KeyError, match="Table 'users' doesn't exist"):
            km._load_existing_pairs()

    def test_build_bk_column(self, mock_conn):
        """Test business key column construction."""
        df = pd.DataFrame({
            "name": ["alice", "bob"], 
            "age": [25, None]
        })
        km = KeyManager("users", mock_conn, df)
        km.set_business_key(["name", "age"])
        
        km._build_bk_column()
        
        expected_bks = ["alice||25", "bob||"]
        assert km.df_incoming["bk_users"].tolist() == expected_bks



    @pytest.mark.parametrize("name,age", test_data.combine("str_normal", "int_normal").get_tuple())
    def test_bk_construction_scenarios(self, mock_conn, name, age):
        """Test BK construction with various data types."""
        df = pd.DataFrame({"name": [name], "age": [age]})
        km = KeyManager("test", mock_conn, df)
        km.set_business_key(["name", "age"])
        
        km._build_bk_column()
        
        # Verify BK was created
        assert "bk_test" in km.df_incoming.columns
    
    def test_prepare_full_workflow(self, mock_conn, mock_existing_pairs):
        """Test complete prepare workflow."""
        with patch('pandas.read_sql', return_value=mock_existing_pairs):
            df = pd.DataFrame({"name": ["alice", "david"], "age": [25, 30]})
            km = KeyManager("users", mock_conn, df)
            km.set_business_key(["name", "age"])
            
            result = km.prepare()
            
            # Verify workflow completed
            assert km._prepared is True
            assert "bk_users" in km.df_incoming.columns
            assert "key_users" in km.df_incoming.columns
            assert result is km
    
    # test_key_dimension.py
    def test_assign_new_keys_empty_table(self, mock_conn):
        """Test key assignment when dimension table is empty."""
        with patch('pandas.read_sql', return_value=pd.DataFrame()):
            df = pd.DataFrame({"name": ["alice", "bob"]})
            kd = KeyDimension("users", mock_conn, df)
            kd.set_business_key(["name"])
            kd.prepare()
            
            # Should assign keys starting from 1
            assert kd.df_incoming["key_users"].tolist() == [1, 2]


#def test_set_business_key(self, columns: Sequence[str])
#def test__build_bk_column(self)
#def test__load_existing_pairs(self)
#def test__assert_no_bk_conflicts(df_pairs: pd.DataFrame, bk_col: str, pk_col: str)
#def test__left_join_existing(self)
#def test_prepare(self)
#def test_result(self)
#
##Tests for KeyDimension:
#def test___init__(
#def test__assign_new_keys(self)
#def test_prepare(self)
#def test_persist(self)
#
##Tests for KeyFact:
#def test___init__(
#def test_register_dimension(
#def test_register_all_dimension(self, *dim_names)
#def test_prepare(self)
#def test_map_dimensions(self, fail_on_missing: bool = True)