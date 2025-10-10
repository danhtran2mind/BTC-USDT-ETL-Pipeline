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

address_port = 12345
web_port = 12346

# Start MinIO server in background
command = f'./minio server ~/minio-data --address ":{address_port}" --console-address ":{web_port}" &'
try:
    subprocess.run(command, shell=True, check=True)
    print(f"MinIO started with API on :{address_port} and WebUI on :{web_port}")
except subprocess.CalledProcessError as e:
    print(f"Failed to start MinIO: {e}")

# Optional ngrok tunneling
use_ngrok = True  # Set this based on your needs

if use_ngrok:
    from pyngrok import ngrok
    load_dotenv("tunneling.env")
    NGROK_AUTHTOKEN = os.getenv("NGROK_TOKEN")
    ngrok.set_auth_token(NGROK_AUTHTOKEN)

    tunnel = ngrok.connect(
        addr=address_port,    # local service port
        proto="http",         # creates both http & https URLs
        bind_tls=True         # forces HTTPS (ngrok’s TLS termination)
    )

    print("Public HTTPS URL →", tunnel.public_url)   # e.g. https://abcd1234.ngrok.io
    print("Public HTTP URL  →", tunnel.config["addr"])  # raw address, usually same host