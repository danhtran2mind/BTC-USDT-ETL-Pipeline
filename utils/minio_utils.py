from io import BytesIO

# Fetch CSV data from MinIO
def get_minio_csv(minio_client, bucket_name="minio-ngrok-bucket",
                    file_name="input.csv"):
    """Get a response from MinIO for the specified bucket and object."""
    # Read file from MinIO
    try:
        response = minio_client.get_object(bucket_name, file_name)
        data = response.read()
    finally:
        response.close()
        response.release_conn()
    # Convert data to BytesIO
    data_io = BytesIO(data)
    # Decode bytes to string and split into lines
    csv_lines = data_io.read().decode('utf-8').splitlines()

    return csv_lines

def get_minio_data(minio_client, bucket_name, file_name):
    """
    Fetch CSV data from MinIO.
    
    Args:
        minio_client: MinIO client instance
        bucket_name (str): Name of the MinIO bucket
        file_name (str): Name of the CSV file
    
    Returns:
        list: List of CSV lines
    """
    return get_minio_csv(minio_client, bucket_name, file_name)