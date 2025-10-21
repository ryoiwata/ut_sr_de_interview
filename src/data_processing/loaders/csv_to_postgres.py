#!/usr/bin/env python3
"""
CSV to PostgreSQL Data Loader

This script reads CSV files and loads them into corresponding PostgreSQL tables.
It handles data type conversion, foreign key relationships, and error handling.

Usage:
    python csv_to_postgres_loader.py [--config config.json] [--data-dir data/]

Requirements:
    - pandas
    - psycopg2-binary
    - python-dotenv (optional, for environment variables)
"""

import pandas as pd
import psycopg2
import psycopg2.extras
import os
import sys
import argparse
from pathlib import Path
from typing import Dict, Any, Optional
import logging
from utils.config import load_config, get_default_config, validate_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CSVToPostgresLoader:
    """Class to handle CSV to PostgreSQL data loading operations."""
    
    def __init__(self, db_config: Dict[str, Any], data_dir: str = "data"):
        """
        Initialize the loader with database configuration.
        
        Args:
            db_config: Database connection parameters
            data_dir: Directory containing CSV files
        """
        self.db_config = db_config
        self.data_dir = Path(data_dir)
        self.connection = None
        
        # Define table loading order (respecting foreign key constraints)
        self.table_load_order = [
            'bio_vip',
            'bio_email', 
            'bio_name',
            'bio_email_for_vip',
            'bio_email_for_vip_tags'
        ]
        
        # Define CSV file mappings
        self.csv_mappings = {
            'bio_vip': 'BIO_VIP.csv',
            'bio_email': 'BIO_EMAIL.csv',
            'bio_name': 'BIO_NAME.csv',
            'bio_email_for_vip': 'BIO_EMAIL_FOR_VIP.csv',
            'bio_email_for_vip_tags': 'BIO_EMAIL_FOR_VIP_TAGS.csv'
        }
    
    def connect(self):
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = False
            logger.info("Successfully connected to PostgreSQL database")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def clean_data(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Clean and prepare data for insertion.
        
        Args:
            df: DataFrame to clean
            table_name: Target table name
            
        Returns:
            Cleaned DataFrame
        """
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Handle empty strings and convert to None
        df = df.replace('', None)
        
        # Table-specific data cleaning
        if table_name == 'bio_vip':
            # Convert timestamps
            for col in ['created_on', 'updated_on']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            # Convert ID column
            if 'id' in df.columns:
                df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
        
        elif table_name == 'bio_email':
            # Convert timestamps
            if 'updated_on' in df.columns:
                df['updated_on'] = pd.to_datetime(df['updated_on'], errors='coerce')
            # Convert ID column
            if 'id' in df.columns:
                df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
        
        elif table_name == 'bio_name':
            # Convert timestamps
            for col in ['created_on', 'updated_on']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            # Convert ID columns
            for col in ['id', 'vip_id']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        
        elif table_name == 'bio_email_for_vip':
            # Convert is_bad to integer
            if 'is_bad' in df.columns:
                df['is_bad'] = pd.to_numeric(df['is_bad'], errors='coerce').astype('Int64')
            # Convert ID columns to regular Python int
            for col in ['id', 'email_id', 'vip_id']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        
        elif table_name == 'bio_email_for_vip_tags':
            # Convert ID columns
            for col in ['id', 'emailforvip_id']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        
        return df
    
    def load_table(self, table_name: str) -> int:
        """
        Load data for a specific table.
        
        Args:
            table_name: Name of the table to load
            
        Returns:
            Number of rows inserted
        """
        csv_file = self.csv_mappings.get(table_name)
        if not csv_file:
            logger.warning(f"No CSV mapping found for table: {table_name}")
            return 0
        
        csv_path = self.data_dir / csv_file
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return 0
        
        try:
            logger.info(f"Loading data for table: {table_name}")
            
            # Read CSV file with header in first row
            df = pd.read_csv(csv_path, header=0)
            
            # Clean the data
            df = self.clean_data(df, table_name)
            
            if df.empty:
                logger.warning(f"No data to load for table: {table_name}")
                return 0
            
            # Get column names from the first row (which is now the header)
            columns = df.columns.tolist()
            
            # Prepare data for insertion with proper type conversion
            data_tuples = []
            for row in df.values:
                converted_row = []
                for value in row:
                    if pd.isna(value):
                        converted_row.append(None)
                    elif hasattr(value, 'item'):  # numpy scalar
                        converted_row.append(value.item())
                    else:
                        converted_row.append(value)
                data_tuples.append(tuple(converted_row))
            
            # Create INSERT statement
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])}
            """
            
            # Execute insert
            cursor = self.connection.cursor()
            cursor.executemany(insert_query, data_tuples)
            rows_inserted = cursor.rowcount
            cursor.close()
            
            logger.info(f"Successfully loaded {rows_inserted} rows into {table_name}")
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Error loading table {table_name}: {e}")
            raise
    
    def load_all_tables(self) -> Dict[str, int]:
        """
        Load all tables in the correct order.
        
        Returns:
            Dictionary with table names and row counts
        """
        results = {}
        
        try:
            for table_name in self.table_load_order:
                rows_inserted = self.load_table(table_name)
                results[table_name] = rows_inserted
                
            # Commit all changes
            self.connection.commit()
            logger.info("All data loaded successfully")
            
        except Exception as e:
            logger.error(f"Error during data loading: {e}")
            self.connection.rollback()
            raise
        
        return results
    
    def verify_data(self) -> Dict[str, int]:
        """
        Verify loaded data by counting rows in each table.
        
        Returns:
            Dictionary with table names and row counts
        """
        results = {}
        cursor = self.connection.cursor()
        
        try:
            for table_name in self.table_load_order:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                results[table_name] = count
                logger.info(f"Table {table_name}: {count} rows")
        
        except Exception as e:
            logger.error(f"Error verifying data: {e}")
            raise
        finally:
            cursor.close()
        
        return results




def main():
    """Main function to handle command line arguments and execute data loading."""
    parser = argparse.ArgumentParser(
        description="Load CSV data into PostgreSQL tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python csv_to_postgres_loader.py
    python csv_to_postgres_loader.py --config config.json
    python csv_to_postgres_loader.py --data-dir data/ --host localhost --database mydb
        """
    )
    
    parser.add_argument('--config', '-c', 
                       help='Path to JSON configuration file')
    parser.add_argument('--data-dir', '-d', 
                       default='data',
                       help='Directory containing CSV files (default: data)')
    parser.add_argument('--host', 
                       help='Database host (overrides config file)')
    parser.add_argument('--port', type=int,
                       help='Database port (overrides config file)')
    parser.add_argument('--database', 
                       help='Database name (overrides config file)')
    parser.add_argument('--user', 
                       help='Database user (overrides config file)')
    parser.add_argument('--password', 
                       help='Database password (overrides config file)')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only verify existing data, do not load new data')
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        if args.config:
            config = load_config(args.config)
        else:
            config = get_default_config()
        
        # Validate configuration
        if not validate_config(config):
            logger.error("Invalid configuration. Please check your config file.")
            sys.exit(1)
        
        # Override config with command line arguments
        if args.host:
            config['host'] = args.host
        if args.port:
            config['port'] = args.port
        if args.database:
            config['database'] = args.database
        if args.user:
            config['user'] = args.user
        if args.password:
            config['password'] = args.password
        
        logger.info("CSV to PostgreSQL Data Loader")
        logger.info("=" * 50)
        logger.info(f"Database: {config['database']}@{config['host']}:{config['port']}")
        logger.info(f"Data directory: {args.data_dir}")
        logger.info("")
        
        # Initialize loader
        loader = CSVToPostgresLoader(config, args.data_dir)
        
        # Connect to database
        loader.connect()
        
        if args.verify_only:
            # Only verify existing data
            logger.info("Verifying existing data...")
            results = loader.verify_data()
        else:
            # Load all tables
            logger.info("Loading CSV data into PostgreSQL...")
            results = loader.load_all_tables()
            
            # Verify loaded data
            logger.info("\nVerifying loaded data...")
            verify_results = loader.verify_data()
        
        # Print summary
        logger.info("\nSummary:")
        logger.info("=" * 50)
        for table_name, count in results.items():
            logger.info(f"{table_name}: {count} rows")
        
        if not args.verify_only:
            logger.info(f"\nTotal tables processed: {len(results)}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'loader' in locals():
            loader.disconnect()


if __name__ == "__main__":
    main()
