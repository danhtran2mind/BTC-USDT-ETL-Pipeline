import os
import sys

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from minio_api.client import sign_in, upload_file, download_file, create_bucket, list_objects

# def up_to_minio(client_files, server_files, bucket_name="minio-ngrok-bucket"):
#     """Upload the local CSV file to MinIO."""

#     for client_file, server_file in zip(client_files, server_files):
#         # Check if local file exists
#         if not os.path.exists(client_file):
#             raise FileNotFoundError(f"Local file {client_file} does not exist")
        
#         minio_client = sign_in()
#         # Create bucket
#         create_bucket(minio_client, bucket_name)

#         # Upload file
#         upload_file(minio_client, bucket_name, client_file, server_file)

def up_to_minio(client_files, bucket_name="minio-ngrok-bucket"):
    """Upload the local CSV files to MinIO."""
    # Ensure client_files is a list
    if not isinstance(client_files, list):
        client_files = [client_files]

    # Derive server_files from client_files
    server_files = [os.path.basename(client_file) for client_file in client_files]

    for client_file, server_file in zip(client_files, server_files):
        # Check if local file exists
        if not os.path.exists(client_file):
            raise FileNotFoundError(f"Local file {client_file} does not exist")
        
        minio_client = sign_in()
        # Create bucket
        create_bucket(minio_client, bucket_name)

        # Upload file
        upload_file(minio_client, bucket_name, client_file, server_file)
    
    return server_files  # Optional: return server_files for downstream tasks

if __name__ == "__main__":
    # Example usage
    try:
        up_to_minio(["temp/BTCUSDT-1s-2025-09.csv"], 
                    ["BTCUSDT-1s-2025-09.csv"], 
                    "minio-ngrok-bucket")
        print("File uploaded successfully.")
    except Exception as e:
        print(f"Error uploading file: {e}")