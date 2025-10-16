import os
import subprocess
import argparse

def install_minio():
    # Download MinIO
    os.system('wget https://dl.min.io/server/minio/release/linux-amd64/minio')
    os.system('chmod +x minio')
    os.system('mkdir -p ~/minio-data')

    # # Set MinIO credentials
    # os.environ['MINIO_ROOT_USER'] = 'username'
    # os.environ['MINIO_ROOT_PASSWORD'] = 'username_password'


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Install and start MinIO server with custom ports")
    args = parser.parse_args()

    install_minio()