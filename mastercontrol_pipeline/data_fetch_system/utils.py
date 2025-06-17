# utils.py
"""Utility functions for common operations."""

import json
import datetime
import logging
import pytz
from dateutil.parser import parse


def reformat_datetime(date_str):
    """
    Convert a UTC datetime string to Eastern Time and reformat to 'M/D/YYYY H:MM'.
    Returns the reformatted string or the original string if parsing fails.
    """
    try:
        # Parse and localize to UTC
        dt_utc = parse(date_str)
        if dt_utc.tzinfo is None:
            dt_utc = pytz.utc.localize(dt_utc)
        else:
            dt_utc = dt_utc.astimezone(pytz.utc)

        # Convert to Eastern Time (auto-handles EST/EDT)
        eastern = pytz.timezone("America/New_York")
        dt_et = dt_utc.astimezone(eastern)

        # Format without leading zeros
        return f"{dt_et.month}/{dt_et.day}/{dt_et.year} {dt_et.hour}:{dt_et.strftime('%M')}"
    except Exception as e:
        logging.error(f"Error reformatting datetime {date_str}: {e}")
        return date_str


def ensure_required_columns(df, required_columns):
    """Add missing columns with empty string values."""
    current_columns = df.columns.tolist()
    for col in required_columns:
        if col not in current_columns:
            df[col] = ''
    return df
