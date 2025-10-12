import pytest
from unittest.mock import patch, Mock
import pandas as pd
from sqlalchemy.engine import Connection

from src.KeyManager import KeyManager, KeyDimension, KeyFact, set_business_key
from CaseGen import CaseGen, BUILTINS

BK_SEP = "||"
DEFAULT_PK_VALUE = -1
DEFAULT_PK_PREFIX = "key" #TODO
DEFAULT_BK_PREFIX = "bk" #TODO
MAX_SAMPLE_CONFLICTS = 10 #TODO
MAX_SAMPLE_ROWS = 20 #TODO

@pytest.fixture
def mock_conn():
    """Mock database connection."""
    return Mock(spec=Connection)

@pytest.fixture
def mock_read_sql():
    """Mock for pandas.read_sql function."""
    return Mock()

@pytest.fixture
def test_data():
    """Mock for pandas.read_sql function."""
    return CaseGen().add_dict(BUILTINS)

class TestBuisnessKey:

    def test_set_business_key_ValueError(self, test_data):
        df = test_data.combine("str_normal", "int_normal").get_df()
        with pytest.raises(ValueError, match="Must provide either bk_name or table_name"):
            set_business_key(df, "str_normal", "int_normal")

    def test_set_business_key_custom_deault(self, test_data):
            df = test_data.combine("str_normal", "int_normal").get_df()
            df_result = set_business_key(df, "str_normal", "int_normal", table_name="user")
            expected = (df["str_normal"].astype(str) + "||" + df["int_normal"].astype(str))
            expected.name = "bk_user"
        
            pd.testing.assert_series_equal(df_result, expected)

    def test_set_business_key_costum_bk_name(self, test_data):
            df = test_data.combine("str_normal", "int_normal").get_df()
            df_result = set_business_key(df, "str_normal", "int_normal", bk_name="bk_cust")
            expected = (df["str_normal"].astype(str) + "||" + df["int_normal"].astype(str))
            expected.name = "bk_cust"
        
            pd.testing.assert_series_equal(df_result, expected)
            assert expected.name in df_result.name

    def test_set_business_key_mixed_col(self, test_data):
            df = test_data.combine("str_normal", "int_mixed").get_df()
            df_result = set_business_key(df, "str_normal", "int_mixed", table_name="user")
            expected = (df["str_normal"].astype(str) + "||" + df["int_mixed"].astype(str))
            expected.name = "bk_user"

            pd.testing.assert_series_equal(df_result, expected)

class TestKeyManager:

    def test_init_defaults(self, test_data, mock_conn):
        df = test_data.combine("str_normal", "int_mixed").get_df()
        km = KeyManager("correct", mock_conn, df)
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, test_data, mock_conn):
        df = test_data.combine("str_normal", "int_normal").get_df()
        km = KeyManager("correct", mock_conn, df, bk_name="bk_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, test_data, mock_conn):
        df = test_data.combine("str_normal", "int_normal").get_df()
        km = KeyManager("correct", mock_conn, df, pk_name="key_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_test"
        assert km.bk_name == "bk_correct"
    
    @patch('pandas.read_sql')
    def test_load_existing_pairs_default(self, mock_read_sql, mock_conn, test_data):
        
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        mock_existing_pairs = (
             test_data.combine("str_dif", "int_dif", "str_mixed")
                .get_df()
                .rename(columns=rename_for_test)
        )

        mock_read_sql.return_value = mock_existing_pairs

        km = KeyManager("correct", mock_conn, mock_existing_pairs)
        df_result = km._load_existing_pairs()
        mock_read_sql.assert_called_once_with(
            "SELECT bk_correct, key_correct FROM correct", 
            mock_conn
        )

        pd.testing.assert_frame_equal(df_result, mock_existing_pairs, check_like=True)

    def test_merge_dimension_table_keys_default(self, mock_conn, test_data):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )
        mock_df_existing_pk_bk_pair = df_existing_dim[["bk_correct", "key_correct"]]
        df_incoming = df_existing_dim[["bk_correct", "val_col"]]

        km = KeyManager("correct", mock_conn, df_incoming)
        #mock_df_existing_pk_bk_pair = km._load_existing_pairs() #In reality there this happens internaly in the class
        km.merge_dimension_keys(mock_df_existing_pk_bk_pair)
        print(km.df_incoming_modified.columns)

        pd.testing.assert_frame_equal(km.df_incoming_modified, df_existing_dim, check_like=True)

class TestKeyDimension:

    def test_init_defaults(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_existing_dim[["bk_correct", "val_col"]]

        km = KeyDimension("correct", mock_conn, df_incoming)
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_test", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_existing_dim[["bk_test", "val_col"]]

        km = KeyDimension("correct", mock_conn, df_incoming, bk_name="bk_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_test", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )
        
        df_incoming = df_existing_dim[["bk_correct", "val_col"]]
        
        km = KeyDimension("correct", mock_conn, df_incoming, pk_name="key_test")
        
        assert km.table_name == "correct"
        assert km.pk_name == "key_test"
        assert km.bk_name == "bk_correct"

    def test_check_bk_in_incoming_df(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_test", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_existing_dim[["bk_test", "val_col"]]
         
        with pytest.raises(ValueError, match="Business key column 'bk_correct' not found in incoming dataframe"):
            KeyDimension("correct", mock_conn, df_incoming)
    def test_check_bk_value_no_valid_bk_values(self, test_data, mock_conn):
        "Tests existience of valid bk values"
        rename_for_test = {"str_none": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_dim_bk_none = ( test_data.combine("str_none", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_dim_bk_none[["bk_correct", "val_col"]]

        with pytest.raises(ValueError, match="No valid business key values found in column"):
            KeyDimension("correct", mock_conn, df_incoming)

    def test_check_bk_value_dublicated_values(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_dubplicated_bk_pk_values = (
             test_data.combine("str_dif", "int_dif", "str_mixed")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_dubplicated_bk_pk_values[["bk_correct", "val_col"]]
         
        with pytest.raises(ValueError, match="Duplicate business keys found in incoming data for table '"):
            KeyDimension("correct", mock_conn, df_incoming)

    @patch.object(KeyDimension, "_load_existing_pairs")
    def test_assign_new_keys_default(self, mock_load_existing_dim, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_existing_dim[["bk_correct", "val_col"]]
        mock_load_existing_dim.return_value = df_existing_dim.iloc[0:3][["bk_correct", "key_correct"]]
        
        km = KeyDimension("correct", mock_conn, df_incoming)
        km.df_existing_pk_bk_pair = km._load_existing_pairs()
        km.merge_dimension_keys(km.df_existing_pk_bk_pair)
        km._assign_new_keys()

        assert km.df_incoming_modified["key_correct"].nunique() == df_existing_dim["key_correct"].nunique()
        assert km.df_incoming_modified["key_correct"].dtype.kind in ['i', 'u']

    def test_assign_new_keys_no_new_keys(self, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )
        df_incoming = df_existing_dim[["bk_correct", "val_col"]]
        df_existing_pk_bk_pair = df_existing_dim[["bk_correct", "key_correct"]]
        
        km = KeyDimension("correct", mock_conn, df_incoming)

        km.merge_dimension_keys(df_existing_pk_bk_pair)
        km._assign_new_keys()

        pd.testing.assert_frame_equal(km.df_incoming_modified, df_existing_dim, check_like=True)
   
 
    @patch.object(KeyDimension, "_load_existing_pairs")
    def test_assign_new_keys_key_col_not_int(self, mock_load_existing_dim, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "val_col", "str_mixed": "key_correct"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )

        df_incoming = df_existing_dim[["bk_correct", "val_col"]]
        df_existing_pk_bk_pair = df_existing_dim[["bk_correct", "key_correct"]]
        mock_load_existing_dim.return_value = df_existing_pk_bk_pair 
        km = KeyDimension("correct", mock_conn, df_incoming)

        km.df_existing_pk_bk_pair =  km._load_existing_pairs()  
        km.merge_dimension_keys(df_existing_pk_bk_pair)


        with pytest.raises(ValueError, match=f"Table {km.table_name} contains non-numeric PKs"):
            km._assign_new_keys()

    @patch("pandas.read_sql")
    def test__call__default(self, mock_pd_read_sql, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )        

        df_incoming = df_existing_dim[["bk_correct", "val_col"]]

        mock_pd_read_sql.return_value = df_existing_dim[["bk_correct", "key_correct"]]

        km = KeyDimension("correct", mock_conn, df_incoming)
        km()
        mock_pd_read_sql.assert_called_once_with(
            "SELECT bk_correct, key_correct FROM correct",
            mock_conn
        )

        pd.testing.assert_frame_equal(km.df_incoming_modified, df_existing_dim, check_like=True)

    @patch("pandas.read_sql")
    def test_process_default(self, mock_pd_read_sql, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )    
        df_incoming = df_existing_dim[["bk_correct", "val_col"]]
        mock_pd_read_sql.return_value = df_existing_dim[["bk_correct", "key_correct"]]

        km = KeyDimension("correct", mock_conn, df_incoming)
        df_result = km.process()

        mock_pd_read_sql.assert_called_once_with(
            "SELECT bk_correct, key_correct FROM correct",
            mock_conn
        )
        pd.testing.assert_frame_equal(df_result, df_existing_dim, check_like=True)

    @patch("pandas.read_sql")
    def test_process_called_twice(self, mock_pd_read_sql, test_data, mock_conn):
        rename_for_test = {"str_dif": "bk_correct", "int_dif": "key_correct", "str_mixed": "val_col"}
        df_existing_dim = (
             test_data.combine("str_dif", "int_dif", "str_mixed", mode="zip")
                .get_df()
                .rename(columns=rename_for_test)
        )  
 
        df_incoming = df_existing_dim[["bk_correct", "val_col"]]
        mock_pd_read_sql.return_value = df_existing_dim[["bk_correct", "key_correct"]]

        km = KeyDimension("correct", mock_conn, df_incoming)

        df_default_1 = km.process()
        df_default_2 = km.process()

        mock_pd_read_sql.assert_called_once_with(
            "SELECT bk_correct, key_correct FROM correct",
            mock_conn
        )
        assert km._processed == True
        pd.testing.assert_frame_equal(df_default_1, df_default_2, check_like=True)

class TestKeyFact:

    def test_init_defaults(self, test_data, mock_conn):
        df = test_data.combine("str_dif", "int_dif", "str_mixed").get_df()
        km = KeyFact("correct", mock_conn, df)
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_correct"

    def test_init_set_bk(self, test_data, mock_conn):
        df = test_data.combine("str_dif", "int_dif", "str_mixed").get_df()
        km = KeyFact("correct", mock_conn, df, bk_name="bk_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_correct"
        assert km.bk_name == "bk_test"

    def test_init_set_pk(self, test_data, mock_conn):
        df = test_data.combine("str_dif", "int_dif", "str_mixed").get_df()
        km = KeyFact("correct", mock_conn, df, pk_name="key_test")
    
        assert km.table_name == "correct"
        assert km.pk_name == "key_test"
        assert km.bk_name == "bk_correct"
    
    def test_register_dimension_default(self, test_data, mock_conn):
        df = test_data.combine("str_dif", "int_dif", "str_mixed").get_df()
        km = KeyFact("correct", mock_conn, df, pk_name="key_test")
        km.register_dimension("correct")
        
        assert km.dim_mappings["correct"]["dim_table"] == "correct"
        assert km.dim_mappings["correct"]["key_name"] == "key_correct"
        assert km.dim_mappings["correct"]["bk_name"] == "bk_correct"
    
    def test_register_dimension_modified_bk_pk(self, test_data, mock_conn):
        df = test_data.combine("str_dif", "int_dif", "str_mixed").get_df()
        km = KeyFact("correct", mock_conn, df, pk_name="key_test")
        km.register_dimension("correct", bk_name="bk_test", pk_name="key_test")

        assert km.dim_mappings["correct"]["dim_table"] == "correct"
        assert km.dim_mappings["correct"]["key_name"] == "key_test"
        assert km.dim_mappings["correct"]["bk_name"] == "bk_test"

    def test_register_all_dimensions_with_list(self, test_data, mock_conn):
        df = test_data.combine("str_dif", "int_dif", "str_mixed").get_df()
        km = KeyFact("correct", mock_conn, df)
        names = [str(i) for i in range(5)]
        km.register_all_dimensions(*names)

        for name in names:
            assert km.dim_mappings[name]["dim_table"] == name
            assert km.dim_mappings[name]["key_name"] == f"key_{name}"
            assert km.dim_mappings[name]["bk_name"] == f"bk_{name}"

    def test_register_all_dimensions_individual_args(self, test_data, mock_conn):        
        df = test_data.combine("str_dif", "int_dif", "str_mixed").get_df()
        km = KeyFact("correct", mock_conn, df)
        km.register_all_dimensions("users", "products", "stores")

        expected_dims = ["users", "products", "stores"]
        for name in expected_dims:
            assert km.dim_mappings[name]["dim_table"] == name
            assert km.dim_mappings[name]["key_name"] == f"key_{name}"
            assert km.dim_mappings[name]["bk_name"] == f"bk_{name}"

    
    @pytest.fixture
    def fact_df(self):
        """Sample fact DataFrame with business keys."""
        return pd.DataFrame({
            "sale_amount": [100, 200, 300],
            "bk_users": ["user1", "user2", "user3"],
            "bk_products": ["prod1", "prod2", "prod3"]
        })

    @pytest.fixture
    def users_dimension_pairs(self):
        """Mock dimension pairs for users."""
        return pd.DataFrame({
            "bk_users": ["user1", "user2", "user4"],
            "key_users": [101, 102, 104]
        })
    
    @pytest.fixture
    def products_dimension_pairs(self):
        """Mock dimension pairs for products."""
        return pd.DataFrame({
            "bk_products": ["prod1", "prod2", "prod5"],
            "key_products": [201, 202, 205]
        })

    def test_import_dimension_keys_already_processed(self, fact_df, mock_conn):
        """Test that already processed fact returns self without processing."""
        fact = KeyFact("sales", mock_conn, fact_df)
        fact._processed = True
        original_df = fact.df_incoming_modified.copy()
        
        result = fact.import_dimension_keys()
        
        assert result is fact
        pd.testing.assert_frame_equal(fact.df_incoming_modified, original_df, check_like=True)

    def test_import_dimension_keys_no_mappings(self, fact_df, mock_conn):
        """Test error when no dimension mappings are registered."""
        fact = KeyFact("sales", mock_conn, fact_df)
        
        with pytest.raises(RuntimeError, match="Reference to dimension is missing"):
            fact.import_dimension_keys()

    def test_import_dimension_keys_missing_bk_column(self, fact_df, mock_conn):
        """Test error when required business key column is missing."""
        fact = KeyFact("sales", mock_conn, fact_df)
        fact.register_dimension("customers", bk_name="bk_customers")  # Column doesn't exist
        
        with pytest.raises(ValueError, match="Fact BK column 'bk_customers' missing in incoming dataframe"):
            fact.import_dimension_keys()

    @patch.object(KeyFact, '_load_existing_pairs')
    @patch.object(KeyFact, 'merge_dimension_keys')
    def test_import_dimension_keys_success_all_found(self, mock_merge, mock_load, 
                                                   fact_df, mock_conn, users_dimension_pairs):
        """Test successful import when all keys are found."""
        fact = KeyFact("sales", mock_conn, fact_df)
        fact.register_dimension("users")
        
        # Mock the loading and merging
        mock_load.return_value = users_dimension_pairs
        
        # Simulate successful merge - all keys found
        fact.df_incoming_modified["key_users"] = [101, 102, 104]  # No NaN values
        
        result = fact.import_dimension_keys()
        
        # Verify calls
        mock_load.assert_called_once_with(
            dim_table="users",
            pk_name="key_users",
            bk_name="bk_users"
        )
        mock_merge.assert_called_once_with(users_dimension_pairs, "bk_users", "key_users")
        # result
        assert result is fact
        assert fact._processed is True
        assert "bk_users" not in fact.df_incoming_modified.columns  # BK column removed

    @patch.object(KeyFact, '_load_existing_pairs')
    @patch.object(KeyFact, 'merge_dimension_keys')
    def test_import_dimension_keys_fail_on_missing_true(self, mock_merge, mock_load,
                                                       fact_df, mock_conn):
        """Test error when missing keys and fail_on_missing=True."""
        fact = KeyFact("sales", mock_conn, fact_df)
        fact.register_dimension("users")
        
        mock_load.return_value = pd.DataFrame({"bk_users": ["user1"], "key_users": [101]})
        
        # Simulate merge with missing keys
        fact.df_incoming_modified["key_users"] = [101, None, None]  # 2 missing keys
        
        with pytest.raises(ValueError) as exc_info:
            fact.import_dimension_keys(fail_on_missing=True)
        
        error_msg = str(exc_info.value)
        assert "Missing dimension keys for 2 rows" in error_msg
        assert "bk_users -> key_users" in error_msg
        assert "from users" in error_msg

    @patch.object(KeyFact, '_load_existing_pairs')
    @patch.object(KeyFact, 'merge_dimension_keys')
    def test_import_dimension_keys_fail_on_missing_false(self, mock_merge, mock_load,
                                                        fact_df, mock_conn):
        """Test default PK assignment when fail_on_missing=False."""
        fact = KeyFact("sales", mock_conn, fact_df)
        fact.register_dimension("users")
        
        mock_load.return_value = pd.DataFrame({"bk_users": ["user1"], "key_users": [101]})
        
        # Simulate merge with missing keys
        fact.df_incoming_modified["key_users"] = [101, None, None]
        
        result = fact.import_dimension_keys(fail_on_missing=False)
        
        # Verify missing keys got default value
        expected_keys = [101, DEFAULT_PK_VALUE, DEFAULT_PK_VALUE]
        assert fact.df_incoming_modified["key_users"].tolist() == expected_keys
        assert result is fact
        assert fact._processed is True

    @patch.object(KeyFact, '_load_existing_pairs')
    @patch.object(KeyFact, 'merge_dimension_keys')
    def test_import_dimension_keys_multiple_dimensions(self, mock_merge, mock_load,
                                                      fact_df, mock_conn,
                                                      users_dimension_pairs,
                                                      products_dimension_pairs):
        """Test importing keys from multiple dimensions."""
        fact = KeyFact("sales", mock_conn, fact_df)
        fact.register_dimension("users")
        fact.register_dimension("products")
        
        # Mock loads for both dimensions
        def mock_load_side_effect(dim_table, pk_name, bk_name):
            if dim_table == "users":
                return users_dimension_pairs
            elif dim_table == "products":
                return products_dimension_pairs
        
        mock_load.side_effect = mock_load_side_effect
        
        # Simulate successful merges
        fact.df_incoming_modified["key_users"] = [101, 102, 104]
        fact.df_incoming_modified["key_products"] = [201, 202, 205]
        
        result = fact.import_dimension_keys()
        
        # Verify both dimensions were processed
        assert mock_load.call_count == 2
        assert mock_merge.call_count == 2
        
        # Verify both BK columns were removed
        assert "bk_users" not in fact.df_incoming_modified.columns
        assert "bk_products" not in fact.df_incoming_modified.columns
        
        # Verify both key columns exist
        assert "key_users" in fact.df_incoming_modified.columns
        assert "key_products" in fact.df_incoming_modified.columns

    @patch.object(KeyFact, '_load_existing_pairs')
    @patch.object(KeyFact, 'merge_dimension_keys')
    def test_import_dimension_keys_partial_missing(self, mock_merge, mock_load,
                                                  fact_df, mock_conn):
        """Test mixed scenario with some found and some missing keys."""
        fact = KeyFact("sales", mock_conn, fact_df)
        fact.register_dimension("users")
        
        mock_load.return_value = pd.DataFrame({"bk_users": ["user1", "user2"], "key_users": [101, 102]})
        
        # Simulate partial match - user3 not found
        fact.df_incoming_modified["key_users"] = [101, 102, None]
        
        result = fact.import_dimension_keys(fail_on_missing=False)
        
        # Verify mixed results
        expected_keys = [101, 102, DEFAULT_PK_VALUE]
        assert fact.df_incoming_modified["key_users"].tolist() == expected_keys

    def test_import_dimension_keys_integration(self, mock_conn):
        """Integration test with realistic data flow."""
        # Create fact data
        fact_df = pd.DataFrame({
            "order_id": [1, 2, 3],
            "amount": [100, 200, 150],
            "bk_customers": ["cust_A", "cust_B", "cust_C"],
            "bk_products": ["prod_X", "prod_Y", "prod_Z"]
        })
        
        fact = KeyFact("orders", mock_conn, fact_df)
        fact.register_dimension("customers", bk_name="bk_customers", pk_name="key_customers")
        fact.register_dimension("products", bk_name="bk_products", pk_name="key_products")
        
        # Mock dimension data
        customer_pairs = pd.DataFrame({
            "bk_customers": ["cust_A", "cust_B"],  # cust_C missing
            "key_customers": [501, 502]
        })
        
        product_pairs = pd.DataFrame({
            "bk_products": ["prod_X", "prod_Y", "prod_Z"],
            "key_products": [701, 702, 703]
        })
        
        with patch.object(fact, '_load_existing_pairs') as mock_load:
            def load_side_effect(dim_table, pk_name, bk_name):
                if dim_table == "customers":
                    return customer_pairs
                elif dim_table == "products":
                    return product_pairs
            
            mock_load.side_effect = load_side_effect
            
            # Process with missing customer allowed
            result = fact.import_dimension_keys(fail_on_missing=False)
            
            # Verify final result
            assert "key_customers" in result.df_incoming_modified.columns
            assert "key_products" in result.df_incoming_modified.columns
            assert "bk_customers" not in result.df_incoming_modified.columns
            assert "bk_products" not in result.df_incoming_modified.columns
            
            # Check key assignments
            customer_keys = result.df_incoming_modified["key_customers"].tolist()
            product_keys = result.df_incoming_modified["key_products"].tolist()
            
            assert customer_keys == [501, 502, DEFAULT_PK_VALUE]  # cust_C gets default
            assert product_keys == [701, 702, 703]  # All products found


