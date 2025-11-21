import datetime
import os
import tempfile
import pandas as pd
from airflow.decorators import dag, task
from etl_helpers.minio_utils import (
    upload_to_minio, download_from_minio, list_objects, create_bucket_if_not_exists
)
from etl_helpers.data_loader import (
    download_crimes_full, download_crimes_incremental, download_police_stations
)
from etl_helpers.data_enrichment import enrich_crime_data

default_args = {
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'dagrun_timeout': datetime.timedelta(minutes=60)  # Increased for large data downloads
}


@dag(
    dag_id="etl_with_taskflow",
    description="Chicago Crime Data ETL Pipeline with TaskFlow API",
    default_args=default_args,
    schedule="@monthly",  # Run monthly for incremental updates
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=["ETL", "Chicago", "Crime", "TaskFlow"],
)
def process_etl_taskflow():

    @task.python
    def setup_s3():
        """Setup MinIO buckets with TTL."""
        bucket_name = os.getenv('DATA_REPO_BUCKET_NAME', 'data')
        lifecycle_prefixes = ['0-raw-data/monthly-data/', '0-raw-data/data/', '1-enriched-data/']

        create_bucket_if_not_exists(bucket_name, lifecycle_prefix=lifecycle_prefixes, lifecycle_days=60)
        print(f"S3 setup completed: bucket '{bucket_name}' with 60-day TTL")

    @task.python
    def download_data(**context):
        """Download crime data and police stations from Socrata API."""
        bucket_name = os.getenv('DATA_REPO_BUCKET_NAME', 'data')

        # Determine month folder from execution date
        start_date = context['data_interval_start']
        month_folder = start_date.strftime('%Y-%m')

        crimes_temp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        stations_temp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)

        try:
            print(f"Downloading data for month: {month_folder}")

            # Download crime data for the month
            end_date = context['data_interval_end']
            crimes_df = download_crimes_incremental(start_date, end_date, output_file=crimes_temp.name)

            if len(crimes_df) == 0:
                print("No new crime data found")
                return {'status': 'no_data', 'records': 0}

            # Download police stations
            print("Downloading police stations data")
            stations_df = download_police_stations(output_file=stations_temp.name)

            # Upload to MinIO
            crimes_key = f"0-raw-data/monthly-data/{month_folder}/crimes.csv"
            stations_key = f"0-raw-data/monthly-data/{month_folder}/police_stations.csv"

            upload_to_minio(crimes_temp.name, bucket_name, crimes_key)
            upload_to_minio(stations_temp.name, bucket_name, stations_key)

            print(f"Download completed: {len(crimes_df)} crime records, {len(stations_df)} police stations")
            print(f"Saved to: {crimes_key}")

            return {
                'status': 'success',
                'month_folder': month_folder,
                'crimes_file': crimes_key,
                'stations_file': stations_key,
                'crime_records': len(crimes_df)
            }
        finally:
            if os.path.exists(crimes_temp.name):
                os.remove(crimes_temp.name)
            if os.path.exists(stations_temp.name):
                os.remove(stations_temp.name)


    @task.python
    def merge_data(download_result):
        """Merge new monthly data into rolling 12-month window."""
        if download_result.get('status') == 'no_data':
            print("No data to merge")
            return {'status': 'no_data'}

        bucket_name = os.getenv('DATA_REPO_BUCKET_NAME', 'data')
        run_date = datetime.datetime.now().strftime('%Y-%m-%d')

        # Check if merged file already exists
        merged_prefix = '0-raw-data/data/crimes_12m_'
        existing_merged = list_objects(bucket_name, prefix=merged_prefix)
        is_first_run = len(existing_merged) == 0

        merged_temp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        monthly_temp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)

        try:
            if is_first_run:
                print("First run: downloading full year of data for initial merge")
                merged_df = download_crimes_full(output_file=merged_temp.name)
                print(f"Initial dataset: {len(merged_df)} records")
            else:
                # Load latest merged file
                latest_merged = sorted(existing_merged)[-1]
                print(f"Loading latest merged file: {latest_merged}")
                download_from_minio(bucket_name, latest_merged, merged_temp.name)
                df_existing = pd.read_csv(merged_temp.name)

                # Load new monthly data
                monthly_key = download_result['crimes_file']
                print(f"Loading new monthly data: {monthly_key}")
                download_from_minio(bucket_name, monthly_key, monthly_temp.name)
                df_new = pd.read_csv(monthly_temp.name)

                # Concatenate
                print(f"Merging: {len(df_existing)} existing + {len(df_new)} new records")
                merged_df = pd.concat([df_existing, df_new], ignore_index=True)

                # Filter to last 12 months
                merged_df['date'] = pd.to_datetime(merged_df['date'])
                twelve_months_ago = datetime.datetime.now() - datetime.timedelta(days=365)
                merged_df = merged_df[merged_df['date'] >= twelve_months_ago]
                print(f"After 12-month filter: {len(merged_df)} records")

            # Save merged data
            output_key = f"0-raw-data/data/crimes_12m_{run_date}.csv"
            merged_df.to_csv(merged_temp.name, index=False)
            upload_to_minio(merged_temp.name, bucket_name, output_key)

            print(f"Merged data saved: {output_key}")

            return {
                'status': 'success',
                'merged_file': output_key,
                'records': len(merged_df),
                'run_date': run_date
            }
        finally:
            if os.path.exists(merged_temp.name):
                os.remove(merged_temp.name)
            if os.path.exists(monthly_temp.name):
                os.remove(monthly_temp.name)

    @task.python
    def enrich_data(merge_result, download_result):
        """Add nearest station info and temporal features to crime data."""
        if merge_result.get('status') == 'no_data':
            print("No data to enrich")
            return {'status': 'no_data'}

        bucket_name = os.getenv('DATA_REPO_BUCKET_NAME', 'data')
        merged_file = merge_result['merged_file']
        run_date = merge_result['run_date']

        # Get police stations from monthly_data
        stations_key = download_result['stations_file']

        crimes_temp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        stations_temp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        enriched_temp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)

        try:
            # Download merged file
            print(f"Loading merged crimes from {merged_file}")
            download_from_minio(bucket_name, merged_file, crimes_temp.name)
            crimes_df = pd.read_csv(crimes_temp.name)

            # Download police stations
            print(f"Loading stations from {stations_key}")
            download_from_minio(bucket_name, stations_key, stations_temp.name)
            stations_df = pd.read_csv(stations_temp.name)

            # Enrich data
            print("Enriching crime data with station info and temporal features")
            enriched_df = enrich_crime_data(crimes_df, stations_df, output_file=enriched_temp.name)

            # Upload enriched data with run date
            enriched_key = f"1-enriched-data/crimes_enriched_{run_date}.csv"
            upload_to_minio(enriched_temp.name, bucket_name, enriched_key)

            print(f"Enrichment completed: {len(enriched_df)} records")
            print(f"Saved to: {enriched_key}")

            return {
                'status': 'success',
                'enriched_file': enriched_key,
                'records': len(enriched_df),
                'run_date': run_date
            }
        finally:
            for temp_file in [crimes_temp.name, stations_temp.name, enriched_temp.name]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
        
    @task.python
    def split_data():
        """Split dataset into train and test sets."""
        print("Splitting dataset...")

    @task.python(multiple_outputs=True)
    def process_outliers():
        """Process outliers in the dataset."""
        print("Processing outliers...")

    @task.python(multiple_outputs=True)
    def encode_data():
        """Encode categorical variables."""
        print("Applying encoding...")

    @task.python(multiple_outputs=True)
    def scale_data():
        """Scale numerical features."""
        print("Applying scaling...")

    @task.python(multiple_outputs=True)
    def balance_data():
        """Balance dataset using SMOTE and undersampling."""
        print("Balancing dataset...")

    @task.python(multiple_outputs=True)
    def extract_features():
        """Extract and select relevant features."""
        print("Extracting features...")

    # Task dependencies
    s3_setup = setup_s3()
    downloaded = download_data()
    merged = merge_data(downloaded)
    enriched = enrich_data(merged, downloaded)
    split = split_data()
    outliers = process_outliers()
    encoded = encode_data()
    scaled = scale_data()
    balanced = balance_data()
    features = extract_features()

    s3_setup >> downloaded >> merged >> enriched >> split >> outliers >> encoded >> scaled >> balanced >> features

dag = process_etl_taskflow()
