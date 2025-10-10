import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

def sign_in():
    """Initialize and return a MinIO client using environment variables."""
    # Load environment variables from minio.env file
    load_dotenv("minio.env")

    # Retrieve MinIO credentials and host from environment variables
    minio_host = os.getenv("MINIO_HOST")
    minio_root_user = os.getenv("MINIO_ROOT_USER")
    minio_root_password = os.getenv("MINIO_ROOT_PASSWORD")

    # Check if environment variables are set
    if not all([minio_host, minio_root_user, minio_root_password]):
        raise ValueError("Missing required environment variables: MINIO_HOST, MINIO_ROOT_USER, or MINIO_ROOT_PASSWORD")

    # Initialize MinIO client
    try:
        minio_client = Minio(
            endpoint=minio_host,
            access_key=minio_root_user,
            secret_key=minio_root_password,
            secure=True  # Set to False if not using HTTPS
        )
        print("Successfully connected to MinIO server")
        return minio_client
    except S3Error as err:
        print(f"Failed to connect to MinIO server: {err}")
        raise

def create_bucket(minio_client, bucket_name):
    """Create a bucket if it doesn't exist."""
    try:
        found = minio_client.bucket_exists(bucket_name)
        if not found:
            minio_client.make_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")
        else:
            print(f"Bucket {bucket_name} already exists")
        return True
    except S3Error as err:
        print(f"Error creating bucket: {err}")
        return False

def upload_file(minio_client, bucket_name, client_file, server_file):
    """Upload a file to the specified bucket."""
    try:
        minio_client.fput_object(bucket_name, server_file, client_file)
        print(f"{client_file} successfully uploaded as {server_file} to bucket {bucket_name}")
        return True
    except S3Error as err:
        print(f"Error uploading file: {err}")
        return False

def download_file(minio_client, bucket_name, object_name, destination_path):
    """Download a file from the specified bucket."""
    try:
        minio_client.fget_object(bucket_name, object_name, destination_path)
        print(f"{object_name} successfully downloaded to {destination_path}")
        return True
    except S3Error as err:
        print(f"Error downloading file: {err}")
        return False

def list_objects(minio_client, bucket_name):
    """List all objects in the specified bucket."""
    try:
        objects = minio_client.list_objects(bucket_name, recursive=True)
        object_list = [obj.object_name for obj in objects]
        if object_list:
            print(f"Objects in bucket {bucket_name}: {object_list}")
        else:
            print(f"No objects found in bucket {bucket_name}")
        return object_list
    except S3Error as err:
        print(f"Error listing objects: {err}")
        return []

# Example usage
if __name__ == "__main__":
    # Configuration
    bucket_name = "minio-ngrok-bucket"
    client_file = "/content/sample_data/mnist_test.csv"
    server_file = "mnist_test.csv"
    download_path = "/content/sample_data/downloaded_mnist_test.csv"

    # Initialize MinIO client
    try:
        minio_client = sign_in()

        # Create bucket
        create_bucket(minio_client, bucket_name)

        # Upload file
        upload_file(minio_client, bucket_name, client_file, server_file)

        # List objects in bucket
        list_objects(minio_client, bucket_name)

        # Download file
        download_file(minio_client, bucket_name, server_file, download_path)

    except Exception as e:
        print(f"An error occurred: {e}")