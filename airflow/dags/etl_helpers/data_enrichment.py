"""
Data Enrichment Functions

Functions for enriching Chicago crime data with geospatial and temporal features.
"""

import os
import pandas as pd
import geopandas as gpd
import logging

logger = logging.getLogger(__name__)


def calculate_nearest_station(crimes_df, stations_df):
    """
    Calculate distance from each crime to the nearest police station.
    Uses GeoPandas spatial join for efficient computation.

    Args:
        crimes_df (pd.DataFrame): Crime data with latitude/longitude
        stations_df (pd.DataFrame): Police stations with latitude/longitude

    Returns:
        pd.DataFrame: Crime data with nearest station distance and district
    """
    logger.info("Calculating distances to nearest police stations...")

    try:
        # Convert crimes to GeoDataFrame
        crimes_gdf = gpd.GeoDataFrame(
            crimes_df,
            geometry=gpd.points_from_xy(
                crimes_df['longitude'].astype(float),
                crimes_df['latitude'].astype(float)
            ),
            crs="EPSG:4326"
        )

        # Convert stations to GeoDataFrame
        stations_gdf = gpd.GeoDataFrame(
            stations_df,
            geometry=gpd.points_from_xy(
                stations_df['longitude'].astype(float),
                stations_df['latitude'].astype(float)
            ),
            crs="EPSG:4326"
        )

        # Perform spatial join to find nearest station
        crimes_with_stations = gpd.sjoin_nearest(
            crimes_gdf,
            stations_gdf[['district', 'district_name', 'geometry']],
            how='left',
            distance_col='distance_crime_to_police_station'
        )

        # Convert back to regular DataFrame
        result_df = pd.DataFrame(crimes_with_stations.drop(columns='geometry'))

        # Rename columns to match expected format
        result_df = result_df.rename(columns={
            'district': 'nearest_police_station_district',
            'district_name': 'nearest_police_station_district_name'
        })

        logger.info(f"Calculated distances for {len(result_df)} crime records")
        return result_df

    except Exception as e:
        logger.error(f"Error calculating nearest stations: {e}")
        raise


def create_temporal_features(df):
    """
    Create temporal features from the date column.
    Creates: Season, Day of Week, Day Time (morning/afternoon/evening/night).

    Args:
        df (pd.DataFrame): DataFrame with 'date' column

    Returns:
        pd.DataFrame: DataFrame with added temporal features
    """
    logger.info("Creating temporal features...")

    try:
        # Convert date to datetime
        df['date'] = pd.to_datetime(df['date'])

        # Extract month and hour
        df['month'] = df['date'].dt.month
        df['hour'] = df['date'].dt.hour
        df['day_of_week'] = df['date'].dt.dayofweek  # 0=Monday, 6=Sunday

        # Create Season feature
        def get_season(month):
            if month in [12, 1, 2]:
                return 'Winter'
            elif month in [3, 4, 5]:
                return 'Spring'
            elif month in [6, 7, 8]:
                return 'Summer'
            else:  # 9, 10, 11
                return 'Fall'

        df['season'] = df['month'].apply(get_season)

        # Create Day Time feature (4 periods)
        def get_day_time(hour):
            if 6 <= hour < 12:
                return 'Morning'
            elif 12 <= hour < 18:
                return 'Afternoon'
            elif 18 <= hour < 24:
                return 'Evening'
            else:  # 0-5
                return 'Night'

        df['day_time'] = df['hour'].apply(get_day_time)

        # Drop temporary columns
        df = df.drop(columns=['month', 'hour'])

        logger.info("Temporal features created: season, day_of_week, day_time")
        return df

    except Exception as e:
        logger.error(f"Error creating temporal features: {e}")
        raise


def clean_and_select_columns(df):
    """
    Clean data and select relevant columns.
    Removes ID columns and unnecessary fields as per notebook.

    Args:
        df (pd.DataFrame): Raw merged dataframe

    Returns:
        pd.DataFrame: Cleaned dataframe with selected columns
    """
    logger.info("Cleaning and selecting columns...")

    try:
        # Define columns to keep (based on notebook 1)
        columns_to_keep = [
            'date',
            'primary_type',
            'description',
            'location_description',
            'arrest',
            'domestic',
            'beat',
            'district',
            'ward',
            'community_area',
            'fbi_code',
            'x_coordinate',
            'y_coordinate',
            'latitude',
            'longitude',
            'distance_crime_to_police_station',
            'nearest_police_station_district',
            'nearest_police_station_district_name',
            'season',
            'day_of_week',
            'day_time'
        ]

        # Keep only columns that exist in the dataframe
        existing_columns = [col for col in columns_to_keep if col in df.columns]
        df_cleaned = df[existing_columns].copy()

        # Convert boolean columns to proper type
        if 'arrest' in df_cleaned.columns:
            df_cleaned['arrest'] = df_cleaned['arrest'].astype(str).str.lower() == 'true'

        if 'domestic' in df_cleaned.columns:
            df_cleaned['domestic'] = df_cleaned['domestic'].astype(str).str.lower() == 'true'

        # Convert numeric columns
        numeric_cols = ['x_coordinate', 'y_coordinate', 'latitude', 'longitude',
                       'distance_crime_to_police_station']
        for col in numeric_cols:
            if col in df_cleaned.columns:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')

        logger.info(f"Cleaned data: {len(df_cleaned)} records, {len(df_cleaned.columns)} columns")
        return df_cleaned

    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
        raise


def enrich_crime_data(crimes_df, stations_df, output_file=None):
    """
    Enrich crime data by adding nearest station info and temporal features.

    Args:
        crimes_df (pd.DataFrame): Downloaded crime data
        stations_df (pd.DataFrame): Police stations data
        output_file (str, optional): Path to save enriched CSV

    Returns:
        pd.DataFrame: Enriched crime data
    """
    logger.info("Starting data enrichment...")

    try:
        if len(crimes_df) == 0:
            logger.warning("No crime data to enrich. Returning empty DataFrame.")
            return pd.DataFrame()

        # Calculate distances to nearest station
        merged_df = calculate_nearest_station(crimes_df, stations_df)

        # Create temporal features
        merged_df = create_temporal_features(merged_df)

        # Clean and select columns
        final_df = clean_and_select_columns(merged_df)

        # Save to file if path provided
        if output_file:
            final_df.to_csv(output_file, index=False)
            file_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
            logger.info(f"Saved enriched data to {output_file} ({file_size:.2f} MB)")

        logger.info(f"Enrichment completed: {len(final_df)} records")
        return final_df

    except Exception as e:
        logger.error(f"Error in data enrichment: {e}")
        raise
