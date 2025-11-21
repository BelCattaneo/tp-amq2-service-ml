"""
Data Loading Functions

Functions for downloading Chicago crime data from Socrata API.
"""

import os
import pandas as pd
from sodapy import Socrata
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Dataset IDs
CRIME_DATASET_ID = "ijzp-q8t2"
POLICE_STATIONS_DATASET_ID = "z8bn-74gv"
SOCRATA_DOMAIN = "data.cityofchicago.org"


def get_socrata_client():
    """
    Initialize Socrata client with app token from environment.

    Returns:
        Socrata: Configured Socrata client
    """
    app_token = os.getenv('SOCRATA_APP_TOKEN')
    if not app_token:
        logger.warning("SOCRATA_APP_TOKEN not found in environment. API rate limits will apply.")

    client = Socrata(
        SOCRATA_DOMAIN,
        app_token,
        timeout=60
    )
    return client


def download_crimes_full(output_file=None):
    """
    Download all crime records from the past year.
    Used for initial/first run when no historical data exists.

    Args:
        output_file (str, optional): Path to save CSV output

    Returns:
        pd.DataFrame: Crime data for the past year
    """
    client = get_socrata_client()

    try:
        # Calculate date range for past year
        one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        today = datetime.now().strftime('%Y-%m-%d')

        logger.info(f"Downloading full crime dataset from {one_year_ago} to {today}...")

        # Download all records with automatic pagination
        results = client.get_all(
            CRIME_DATASET_ID,
            where=f"date >= '{one_year_ago}' AND date <= '{today}'",
            order=":id"
        )

        # Convert to DataFrame
        df = pd.DataFrame.from_records(results)
        logger.info(f"Downloaded {len(df)} crime records (full dataset)")

        # Save to CSV if path provided
        if output_file:
            df.to_csv(output_file, index=False)
            logger.info(f"Saved to {output_file}")

        return df

    except Exception as e:
        logger.error(f"Error downloading full crime dataset: {e}")
        raise
    finally:
        client.close()


def download_crimes_incremental(start_date, end_date, output_file=None):
    """
    Download crime records for a specific date range (incremental update).

    Args:
        start_date (str or datetime): Start date in YYYY-MM-DD format
        end_date (str or datetime): End date in YYYY-MM-DD format
        output_file (str, optional): Path to save CSV output

    Returns:
        pd.DataFrame: Crime data for the specified date range
    """
    client = get_socrata_client()

    try:
        # Convert dates to strings if datetime objects
        if isinstance(start_date, datetime):
            start_date = start_date.strftime('%Y-%m-%d')
        if isinstance(end_date, datetime):
            end_date = end_date.strftime('%Y-%m-%d')

        logger.info(f"Downloading incremental crime data from {start_date} to {end_date}...")

        # Download records for the specified date range
        results = client.get_all(
            CRIME_DATASET_ID,
            where=f"date >= '{start_date}' AND date < '{end_date}'",
            order=":id"
        )

        # Convert to DataFrame
        df = pd.DataFrame.from_records(results)
        logger.info(f"Downloaded {len(df)} crime records (incremental)")

        # Save to CSV if path provided
        if output_file:
            df.to_csv(output_file, index=False)
            logger.info(f"Saved to {output_file}")

        return df

    except Exception as e:
        logger.error(f"Error downloading incremental crime data: {e}")
        raise
    finally:
        client.close()


def download_police_stations(output_file=None):
    """
    Download Chicago police stations dataset.

    Args:
        output_file (str, optional): Path to save CSV output

    Returns:
        pd.DataFrame: Police stations data with coordinates
    """
    client = get_socrata_client()

    try:
        logger.info("Downloading police stations dataset...")

        # Download all police stations (only 23 records)
        results = client.get_all(
            POLICE_STATIONS_DATASET_ID,
            order=":id"
        )

        # Convert to DataFrame
        df = pd.DataFrame.from_records(results)
        logger.info(f"Downloaded {len(df)} police stations")

        # Save to CSV if path provided
        if output_file:
            df.to_csv(output_file, index=False)
            logger.info(f"Saved to {output_file}")

        return df

    except Exception as e:
        logger.error(f"Error downloading police stations: {e}")
        raise
    finally:
        client.close()
