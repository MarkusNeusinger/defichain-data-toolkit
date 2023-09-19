"""
DefiChain Data Helper

This module provides functionality to fetch historical data from the DefiChain Data API.
"""

from datetime import datetime
import pandas as pd
from urllib.parse import urlencode
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


def fetch_history(history_type, features, from_timestamp=None, to_timestamp=None, retries=3, max_worker=8):
    """
    Fetch historical data for a given history type and features.

    Parameters:
    - history_type (str): The type of history data to fetch ('daily', 'hourly', 'minutely').
    - features (list): List of features to fetch.
    - from_timestamp (str, optional): The starting timestamp in 'YYYY-MM-DDTHH:MM:SS' or 'YYYY-MM-DD' format. Defaults to None.
    - to_timestamp (str, optional): The ending timestamp in 'YYYY-MM-DDTHH:MM:SS' or 'YYYY-MM-DD' format. Defaults to None.
    - retries (int, optional): Number of retry attempts. Defaults to 3.
    - max_worker (int, optional): Maximum number of threads for parallel fetching. Defaults to 8.

    Returns:
    - DataFrame: A DataFrame containing the historical data.

    Exceptions:
    - Raises a ValueError if the fetched data does not match expected format.
    """

    # Validate history_type
    if history_type not in ['daily', 'hourly', 'minutely']:
        raise ValueError("Invalid history_type. Must be 'daily', 'hourly', or 'minutely'")

    # Validate timestamps
    allowed_time_formats = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]

    def validate_timestamp(timestamp):
        if timestamp:
            for time_format in allowed_time_formats:
                try:
                    datetime.strptime(timestamp, time_format)
                    return True
                except ValueError:
                    continue
            raise ValueError(f"Invalid timestamp format. Allowed formats are: {', '.join(allowed_time_formats)}")

    validate_timestamp(from_timestamp)
    validate_timestamp(to_timestamp)

    base_url = f'https://api.defichain-data.com/v0/history_{history_type}'
    all_feature_dfs = []

    def fetch_single_feature(feature):
        """Inner function to fetch a single feature"""
        for _ in range(retries):
            try:
                parameters = {
                    'features': feature,
                    'format': 'csv'
                }
                if from_timestamp:
                    parameters['from_timestamp'] = from_timestamp
                if to_timestamp:
                    parameters['to_timestamp'] = to_timestamp

                history_url = f"{base_url}?{urlencode(parameters)}"
                feature_df = pd.read_csv(history_url)

                # Check if the DataFrame has any rows
                if feature_df.shape[0] == 0 or feature_df.shape[1] != 2:
                    raise ValueError("DataFrame is empty or has the wrong number of columns")

                if 'datetime_utc' in feature_df.columns:
                    feature_df['datetime_utc'] = pd.to_datetime(feature_df['datetime_utc'])
                    all_feature_dfs.append(feature_df)
                return feature_df
            except Exception as e:
                print(f"Failed to fetch data for {feature} - retrying. Error: {e}")

        print(f"Failed to fetch data for {feature} after {retries} retries")
        return None

    with ThreadPoolExecutor(max_workers=max_worker) as executor:
        list(tqdm(executor.map(fetch_single_feature, features), total=len(features)))

    # Merge all feature DataFrames
    if all_feature_dfs:
        history_df = pd.concat(all_feature_dfs, axis=0)
        history_df = history_df.groupby('datetime_utc', as_index=False).first()
        history_df = history_df.sort_values('datetime_utc', ascending=False)
        return history_df
    else:
        return None
