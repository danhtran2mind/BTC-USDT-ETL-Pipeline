# import os

# def up_to_data_lake(client_file, server_file, bucket_name="minio-ngrok-bucket"):
#     """Upload the local CSV file to MinIO."""
#     # Check if local file exists
#     if not os.path.exists(client_file):
#         raise FileNotFoundError(f"Local file {client_file} does not exist")
    
#     minio_client = sign_in()
#     # Create bucket
#     create_bucket(minio_client, bucket_name)

#     # Upload file
#     upload_file(minio_client, bucket_name, client_file, server_file)