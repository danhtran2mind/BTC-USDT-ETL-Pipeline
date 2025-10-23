import os
import yaml
from typing import Dict, List, Tuple

def load_config(config_name: str) -> Dict:
    """Load a YAML configuration file from the configs directory.

    Args:
        config_name (str): Name of the config file (e.g., 'model_config.yml').

    Returns:
        Dict: Parsed configuration dictionary.
    """
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'configs', config_name)
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def load_extract_config() -> Tuple[List[str], str]:
    """Load extraction configuration for MinIO files.

    Returns:
        Tuple[List[str], str]: List of file names and storage folder.
    """
    config = load_config('extract_data.yml')
    return config.get('files', []), config.get('storage_folder', 'temp')

def get_parquet_file_names() -> List[str]:
    """Convert CSV file names from extract config to Parquet file names.

    Returns:
        List[str]: List of Parquet file names.
    """
    files, _ = load_extract_config()
    return [f.replace(".csv", ".parquet") for f in files]

def load_pipeline_config() -> Dict:
    """Load pipeline configuration from pipeline_config.yml.

    Returns:
        Dict: Pipeline configuration dictionary.
    """
    return load_config('pipeline_config.yml')

def define_server_filenames(**kwargs) -> List[str]:
    """Extract base filenames from client file paths.

    Args:
        kwargs: Airflow task instance arguments containing 'ti' for XCom.

    Returns:
        List[str]: List of base filenames.
    """
    ti = kwargs['ti']
    client_files = ti.xcom_pull(task_ids='download_binance_csv')
    if not isinstance(client_files, list):
        client_files = [client_files]
    return [os.path.basename(p) for p in client_files]