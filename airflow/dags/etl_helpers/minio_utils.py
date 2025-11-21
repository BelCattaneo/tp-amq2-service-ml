"""
MinIO/S3 Utility Functions

Helper functions for interacting with MinIO storage using boto3.
"""

import os
import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)


def get_minio_client():
    """
    Initialize and return a boto3 S3 client configured for MinIO.

    Reads configuration from environment variables:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - MLFLOW_S3_ENDPOINT_URL (defaults to http://s3:9000)

    Returns:
        boto3.client: Configured S3 client for MinIO
    """
    access_key = os.getenv('AWS_ACCESS_KEY_ID', 'minio')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'minio123')
    endpoint_url = os.getenv('MLFLOW_S3_ENDPOINT_URL', 'http://s3:9000')

    client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='us-east-1'  # MinIO doesn't require specific region
    )

    logger.info(f"MinIO client initialized with endpoint: {endpoint_url}")
    return client


def set_bucket_lifecycle_policy(bucket_name, prefixes, expiration_days):
    """
    Set lifecycle policy to automatically delete objects after specified days.

    Args:
        bucket_name (str): Name of the MinIO bucket
        prefixes (list or str): Prefix(es) for objects to apply policy
        expiration_days (int): Number of days after which objects are deleted

    Returns:
        bool: True if policy set successfully
    """
    client = get_minio_client()

    if isinstance(prefixes, str):
        prefixes = [prefixes]

    rules = []
    for idx, prefix in enumerate(prefixes):
        rules.append({
            'ID': f'Delete-{prefix.replace("/", "-")}-after-{expiration_days}-days',
            'Status': 'Enabled',
            'Filter': {'Prefix': prefix},
            'Expiration': {'Days': expiration_days}
        })

    lifecycle_config = {'Rules': rules}

    try:
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
        logger.info(f"Lifecycle policy set for '{bucket_name}': {len(prefixes)} prefixes with {expiration_days} days TTL")
        return True
    except Exception as e:
        logger.error(f"Error setting lifecycle policy: {e}")
        raise


def create_bucket_if_not_exists(bucket_name, lifecycle_prefix=None, lifecycle_days=None):
    """
    Create a MinIO bucket if it doesn't already exist.
    Optionally set lifecycle policy for automatic file deletion.

    Args:
        bucket_name (str): Name of the bucket to create
        lifecycle_prefix (str, optional): Prefix for lifecycle policy (e.g., 'raw-data/')
        lifecycle_days (int, optional): Days before automatic deletion (TTL)

    Returns:
        bool: True if bucket exists or was created successfully
    """
    client = get_minio_client()

    try:
        client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' already exists")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            try:
                client.create_bucket(Bucket=bucket_name)
                logger.info(f"Bucket '{bucket_name}' created successfully")
            except Exception as create_error:
                logger.error(f"Error creating bucket '{bucket_name}': {create_error}")
                raise
        else:
            logger.error(f"Error checking bucket '{bucket_name}': {e}")
            raise

    if lifecycle_prefix and lifecycle_days:
        set_bucket_lifecycle_policy(bucket_name, lifecycle_prefix, lifecycle_days)

    return True


def upload_to_minio(file_path, bucket_name, object_key):
    """
    Upload a file to MinIO.

    Args:
        file_path (str): Local path to the file to upload
        bucket_name (str): Name of the MinIO bucket
        object_key (str): Key (path) for the object in MinIO

    Returns:
        bool: True if upload successful

    Raises:
        FileNotFoundError: If local file doesn't exist
        ClientError: If upload fails
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    client = get_minio_client()

    try:
        # Ensure bucket exists
        create_bucket_if_not_exists(bucket_name)

        # Upload file
        client.upload_file(file_path, bucket_name, object_key)
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        logger.info(f"Uploaded '{file_path}' ({file_size:.2f} MB) to '{bucket_name}/{object_key}'")
        return True

    except Exception as e:
        logger.error(f"Error uploading file to MinIO: {e}")
        raise


def download_from_minio(bucket_name, object_key, file_path):
    """
    Download a file from MinIO.

    Args:
        bucket_name (str): Name of the MinIO bucket
        object_key (str): Key (path) of the object in MinIO
        file_path (str): Local path where to save the downloaded file

    Returns:
        bool: True if download successful

    Raises:
        ClientError: If download fails or object doesn't exist
    """
    client = get_minio_client()

    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Download file
        client.download_file(bucket_name, object_key, file_path)
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        logger.info(f"Downloaded '{bucket_name}/{object_key}' ({file_size:.2f} MB) to '{file_path}'")
        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.warning(f"Object not found: '{bucket_name}/{object_key}'")
        else:
            logger.error(f"Error downloading file from MinIO: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error downloading file: {e}")
        raise


def check_file_exists(bucket_name, object_key):
    """
    Check if a file exists in MinIO.

    Args:
        bucket_name (str): Name of the MinIO bucket
        object_key (str): Key (path) of the object in MinIO

    Returns:
        bool: True if file exists, False otherwise
    """
    client = get_minio_client()

    try:
        client.head_object(Bucket=bucket_name, Key=object_key)
        logger.info(f"File exists: '{bucket_name}/{object_key}'")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.info(f"File does not exist: '{bucket_name}/{object_key}'")
            return False
        else:
            logger.error(f"Error checking file existence: {e}")
            raise


def list_objects(bucket_name, prefix=''):
    """
    List objects in a MinIO bucket with optional prefix filter.

    Args:
        bucket_name (str): Name of the MinIO bucket
        prefix (str): Prefix to filter objects (e.g., 'raw-data/')

    Returns:
        list: List of object keys matching the prefix
    """
    client = get_minio_client()

    try:
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' not in response:
            logger.info(f"No objects found in '{bucket_name}' with prefix '{prefix}'")
            return []

        objects = [obj['Key'] for obj in response['Contents']]
        logger.info(f"Found {len(objects)} objects in '{bucket_name}' with prefix '{prefix}'")
        return objects

    except Exception as e:
        logger.error(f"Error listing objects: {e}")
        raise


def delete_object(bucket_name, object_key):
    """
    Delete an object from MinIO.

    Args:
        bucket_name (str): Name of the MinIO bucket
        object_key (str): Key (path) of the object to delete

    Returns:
        bool: True if deletion successful
    """
    client = get_minio_client()

    try:
        client.delete_object(Bucket=bucket_name, Key=object_key)
        logger.info(f"Deleted object: '{bucket_name}/{object_key}'")
        return True
    except Exception as e:
        logger.error(f"Error deleting object: {e}")
        raise
