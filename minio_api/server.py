import os
import subprocess
from dotenv import load_dotenv

# Install MinIO binary
subprocess.run(["wget", "https://dl.min.io/server/minio/release/linux-amd64/minio"])
subprocess.run(["chmod", "+x", "minio"])
subprocess.run(["mkdir", "-p", "~/minio-data"])

# Load environment variables
load_dotenv("minio.env")

minio_root_user = os.getenv("MINIO_ROOT_USER")
minio_root_password = os.getenv("MINIO_ROOT_PASSWORD")

address_port = 9192
web_port = 9193

# Start MinIO server in background
command = f'./minio server ~/minio-data --address ":{address_port}" --console-address ":{web_port}" &'
try:
    subprocess.run(command, shell=True, check=True)
    print(f"MinIO started with API on :{address_port} and WebUI on :{web_port}")
except subprocess.CalledProcessError as e:
    print(f"Failed to start MinIO: {e}")

