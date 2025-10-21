"""
Configuration management utilities.

This module provides functions for loading and managing
database configuration settings.
"""

import json
import logging
from typing import Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load database configuration from JSON file.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Dictionary containing configuration parameters
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON
    """
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_file}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing config file: {e}")
        raise


def get_default_config() -> Dict[str, Any]:
    """
    Get default database configuration.
    
    Returns:
        Dictionary with default configuration values
    """
    return {
        "host": "localhost",
        "port": 5432,
        "database": "ut_data_operations",
        "user": "postgres",
        "password": "postgres"
    }


def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validate configuration parameters.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        True if configuration is valid, False otherwise
    """
    required_keys = ['host', 'port', 'database', 'user', 'password']
    
    for key in required_keys:
        if key not in config:
            logger.error(f"Missing required configuration key: {key}")
            return False
    
    # Validate port is a number
    try:
        int(config['port'])
    except (ValueError, TypeError):
        logger.error("Port must be a valid integer")
        return False
    
    return True
