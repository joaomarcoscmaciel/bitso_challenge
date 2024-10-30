import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import yaml
import logging
from schema_validation import validate_schema
from data_type_validation import validate_data_types
from null_checks import check_null_values
from duplicate_checks import check_duplicates
from range_checks import check_range
from consistency_checks import (
    check_user_id_consistency, check_event_type_consistency, 
    check_currency_consistency, check_date_range, 
    check_interface_consistency
)
from utils.logging import setup_logging

# Load configuration
with open('config/config.yml') as file:
    config = yaml.safe_load(file)

# Setup logging
setup_logging(config['log_file'])

# Load the generated fact and dimension tables
fact_withdrawals = pd.read_csv(config['output_data_path'] + 'fact_withdrawals.csv')
fact_deposits = pd.read_csv(config['output_data_path'] + 'fact_deposits.csv')
fact_events = pd.read_csv(config['output_data_path'] + 'fact_events.csv')
dim_user = pd.read_csv(config['output_data_path'] + 'dim_user.csv')
dim_currency = pd.read_csv(config['output_data_path'] + 'dim_currency.csv')
dim_interface = pd.read_csv(config['output_data_path'] + 'dim_interface.csv')
dim_time = pd.read_csv(config['output_data_path'] + 'dim_time.csv')
dim_event_type = pd.read_csv(config['output_data_path'] + 'dim_event_type.csv')

def run_pipeline_tests():
    """Runs the full suite of pipeline validation checks."""
    try:
        logging.info("Starting pipeline validation tests...")

        # Test 1: Schema Validation
        logging.info("Running schema validation...")
        validate_schema(fact_withdrawals, ['id', 'event_timestamp', 'user_id', 'amount', 'interface', 'currency', 'tx_status'], 'Fact_Withdrawals')
        validate_schema(fact_deposits, ['id', 'event_timestamp', 'user_id', 'amount', 'currency', 'tx_status'], 'Fact_Deposits')
        validate_schema(fact_events, ['id', 'event_timestamp', 'user_id', 'event_name'], 'Fact_Events')

        # Test 2: Null Value Checks
        logging.info("\nRunning null value checks...")
        check_null_values(fact_withdrawals, ['user_id', 'event_timestamp', 'amount'], 'Fact_Withdrawals')
        check_null_values(fact_deposits, ['user_id', 'event_timestamp', 'amount'], 'Fact_Events')
        check_null_values(fact_events, ['user_id', 'event_timestamp', 'event_name'], 'Fact_Events')

        # Test 3: Duplicate Checks
        logging.info("\nRunning duplicate checks...")
        check_duplicates(fact_withdrawals, 'id', 'Fact_Withdrawals')
        check_duplicates(fact_deposits, 'id', 'Fact_Deposits')
        check_duplicates(fact_events, 'id', 'Fact_Events')

        # Test 4: Consistency Checks (User IDs, Event Types, Currency)
        logging.info("Running consistency checks...")
        check_user_id_consistency(fact_withdrawals, dim_user, 'Fact_Withdrawals', 'Dim_User')
        check_user_id_consistency(fact_deposits, dim_user, 'Fact_Deposits', 'Dim_User')
        check_user_id_consistency(fact_events, dim_user, 'Fact_Events', 'Dim_User')
        check_event_type_consistency(fact_events, dim_event_type, 'Fact_Events', 'Dim_Event_Type')
        check_currency_consistency(fact_withdrawals, dim_currency, 'Fact_Withdrawals', 'Dim_Currency')
        check_currency_consistency(fact_deposits, dim_currency, 'Fact_Deposits', 'Dim_Currency')
        check_interface_consistency(fact_withdrawals, dim_interface, 'Fact_Withdrawals', 'Dim_Interface')

        # Test 5: Range Checks for Amount
        logging.info("Running range checks for amounts...")
        check_range(fact_withdrawals, 'amount', 0, 1e9, 'Fact_Withdrawals')
        check_range(fact_deposits, 'amount', 0, 1e9, 'Fact_Deposits')

        # Test 6: Data Type Validation
        logging.info("Running data type validation...")
        validate_data_types(fact_withdrawals, {'id': 'int64', 'user_id': 'object', 'amount': 'float64'}, 'Fact_Withdrawals')
        validate_data_types(fact_deposits, {'id': 'int64', 'user_id': 'object', 'amount': 'float64'}, 'Fact_Deposits')

        # Test 7: Date Consistency Checks
        logging.info("Running date consistency checks...")
        check_date_range(fact_withdrawals, 'event_timestamp', min_date='2020-01-01', max_date='2025-01-01', table_name='Fact_Withdrawals')
        check_date_range(fact_deposits, 'event_timestamp', min_date='2020-01-01', max_date='2025-01-01', table_name='Fact_Deposits')
        check_date_range(fact_events, 'event_timestamp', min_date='2020-01-01', max_date='2025-01-01', table_name='Fact_Events')

        logging.info("All pipeline validation tests completed successfully.")
    except Exception as e:
        logging.error(f"Error during pipeline validation tests: {e}")

if __name__ == "__main__":
    run_pipeline_tests()
