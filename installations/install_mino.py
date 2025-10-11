import os
import subprocess
import argparse

def install_and_start_minio(address_port, web_port):
    # Download MinIO
    os.system('wget https://dl.min.io/server/minio/release/linux-amd64/minio')
    os.system('chmod +x minio')
    os.system('mkdir -p ~/minio-data')

    # Set MinIO credentials
    os.environ['MINIO_ROOT_USER'] = 'username'
    os.environ['MINIO_ROOT_PASSWORD'] = 'username_password'

    # Start MinIO server
    command = f'./minio server ~/minio-data --address ":{address_port}" --console-address ":{web_port}" &'

    try:
        subprocess.run(command, shell=True, check=True)
        print(f"MinIO started with API on :{address_port} and WebUI on :{web_port}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to start MinIO: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Install and start MinIO server with custom ports")
    parser.add_argument('--address-port', type=int, default=9000, help='Port for MinIO API')
    parser.add_argument('--web-port', type=int, default=9001, help='Port for MinIO WebUI')
    args = parser.parse_args()

    install_and_start_minio(args.address_port, args.web_port)