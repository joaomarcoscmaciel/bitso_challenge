import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import logging

STATE_FILE = 'last_update.json'

def read_last_update_timestamp():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as file:
            state = json.load(file)
            return state.get('last_update_timestamp', None)
    return None

def update_last_update_timestamp(*dfs):
    # Get the latest timestamp from all processed dataframes
    latest_timestamp = max([df['event_timestamp'].max() for df in dfs if not df.empty])
    state = {'last_update_timestamp': str(latest_timestamp)}
    with open(STATE_FILE, 'w') as file:
        json.dump(state, file)

    logging.info(f"Last update timestamp updated to: {latest_timestamp}")
