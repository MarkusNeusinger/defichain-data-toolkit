import pytest
import pandas as pd
from utils import fetch_history


def test_api_fetch_history_invalid_history_type():
    with pytest.raises(ValueError):
        fetch_history('annually', ['DFI:USD'])


def test_api_fetch_history_invalid_timestamp_format():
    with pytest.raises(ValueError):
        fetch_history('daily', ['DFI:USD'], from_timestamp='invalid-format')


def test_api_fetch_history_daily():
    df = fetch_history('daily', ['DFI:USD'])
    assert isinstance(df, pd.DataFrame)
    assert 'datetime_utc' in df.columns


def test_api_fetch_history_hourly():
    df = fetch_history('hourly', ['DFI:USD'])
    assert isinstance(df, pd.DataFrame)
    assert 'datetime_utc' in df.columns


def test_api_fetch_history_minutely():
    df = fetch_history('minutely', ['DFI:USD'])
    assert isinstance(df, pd.DataFrame)
    assert 'datetime_utc' in df.columns
