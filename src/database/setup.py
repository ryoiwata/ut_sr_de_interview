#!/usr/bin/env python3
"""
Database Setup Script

This script creates the database and tables for the UT Data Operations project.
It should be run before loading CSV data.

Usage:
    python setup_database.py [--config config.json]
"""

import psycopg2
import psycopg2.extensions
import argparse
import sys
import logging
from pathlib import Path
from utils.config import load_config, validate_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)




def create_database(config: dict):
    """Create the database if it doesn't exist."""
    # Connect to default postgres database to create our database
    create_db_config = config.copy()
    create_db_config['database'] = 'postgres'
    
    try:
        conn = psycopg2.connect(**create_db_config)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (config['database'],)
        )
        
        if cursor.fetchone():
            logger.info(f"Database '{config['database']}' already exists")
        else:
            # Create database
            cursor.execute(f"CREATE DATABASE {config['database']}")
            logger.info(f"Database '{config['database']}' created successfully")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        logger.error(f"Error creating database: {e}")
        raise


def create_tables(config: dict):
    """Create tables using the SQL script."""
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Read and execute the SQL script
        sql_file = Path(__file__).parent / "schema" / "create_tables.sql"
        
        if not sql_file.exists():
            logger.error(f"SQL file not found: {sql_file}")
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        
        with open(sql_file, 'r') as f:
            sql_script = f.read()
        
        # Execute the SQL script
        cursor.execute(sql_script)
        logger.info("Tables created successfully")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        logger.error(f"Error creating tables: {e}")
        raise


def main():
    """Main function to set up the database."""
    parser = argparse.ArgumentParser(
        description="Set up PostgreSQL database and tables for UT Data Operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python setup_database.py
    python setup_database.py --config config.json
        """
    )
    
    parser.add_argument('--config', '-c', 
                       default='config.json',
                       help='Path to JSON configuration file (default: config.json)')
    
    args = parser.parse_args()
    
    try:
        logger.info("Database Setup for UT Data Operations")
        logger.info("=" * 50)
        
        # Load configuration
        config = load_config(args.config)
        
        # Validate configuration
        if not validate_config(config):
            logger.error("Invalid configuration. Please check your config file.")
            sys.exit(1)
            
        logger.info(f"Using database: {config['database']}@{config['host']}:{config['port']}")
        
        # Create database
        logger.info("\n1. Creating database...")
        create_database(config)
        
        # Create tables
        logger.info("\n2. Creating tables...")
        create_tables(config)
        
        logger.info("\n✓ Database setup completed successfully!")
        logger.info("You can now run the CSV loader script to populate the tables.")
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
