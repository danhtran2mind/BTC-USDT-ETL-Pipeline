import os
import yaml
import logging
from typing import Dict, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_name: str) -> Dict:
    """Load a YAML configuration file from the configs directory.

    Args:
        config_name (str): Name of the config file (e.g., 'model_config.yml').

    Returns:
        Dict: Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        ValueError: If config_name is empty or not a string.
    """
    if not isinstance(config_name, str) or not config_name.strip():
        raise ValueError("config_name must be a non-empty string")

    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'configs', config_name))
    logging.debug(f"Attempting to load config from: {config_path}")

    if not os.path.exists(config_path):
        logging.error(f"Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        if config is None:
            logging.warning(f"Configuration file {config_name} is empty")
            return {}
        logging.info(f"Successfully loaded config: {config_name}")
        return config
    except yaml.YAMLError as e:
        logging.error(f"Failed to parse YAML in {config_name}: {e}")
        raise

def get_parquet_file_names() -> List[str]:
    """Retrieve Parquet file names from the extract_data.yml configuration.

    Returns:
        List[str]: List of Parquet file names derived from CSV file names.

    Raises:
        FileNotFoundError: If extract_data.yml is missing.
        ValueError: If no files are specified in the configuration.
    """
    config = load_config('extract_data.yml')
    files = config.get('files', [])
    if not files:
        logging.error("No files specified in extract_data.yml")
        raise ValueError("No files specified in extract_data.yml")
    
    parquet_files = [f.replace(".csv", ".parquet") for f in files]
    logging.debug(f"Derived Parquet file names: {parquet_files}")
    return parquet_files

def load_pipeline_config() -> Dict:
    """Load pipeline configuration from pipeline_config.yml.

    Returns:
        Dict: Pipeline configuration dictionary.

    Raises:
        FileNotFoundError: If pipeline_config.yml is missing.
    """
    config = load_config('pipeline_config.yml')
    logging.debug(f"Pipeline config loaded: {config}")
    return config

def define_server_filenames(**kwargs) -> List[str]:
    """Extract base filenames from client file paths using Airflow XCom.

    Args:
        kwargs: Airflow task instance arguments containing 'ti' for XCom.

    Returns:
        List[str]: List of base filenames.

    Raises:
        KeyError: If 'ti' is not provided in kwargs.
        ValueError: If no files are retrieved from XCom.
    """
    if 'ti' not in kwargs:
        logging.error("Task instance 'ti' not provided in kwargs")
        raise KeyError("Task instance 'ti' not provided in kwargs")

    ti = kwargs['ti']
    client_files = ti.xcom_pull(task_ids='download_binance_csv')
    if client_files is None:
        logging.error("No files retrieved from XCom for task 'download_binance_csv'")
        raise ValueError("No files retrieved from XCom for task 'download_binance_csv'")

    if not isinstance(client_files, list):
        client_files = [client_files]
    
    server_files = [os.path.basename(p) for p in client_files]
    logging.debug(f"Extracted server filenames: {server_files}")
    return server_files