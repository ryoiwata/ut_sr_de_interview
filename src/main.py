#!/usr/bin/env python3
"""
Main entry point for UT Data Operations.

This script provides a unified interface for all data operations
including database setup, data conversion, and data loading.
"""

import argparse
import sys
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_database(args):
    """Run database setup."""
    from database.setup import main as setup_main
    sys.argv = ['setup.py', '--config', args.config] if args.config else ['setup.py']
    setup_main()


def convert_excel(args):
    """Convert Excel to CSV."""
    from data_processing.converters.excel_to_csv import main as convert_main
    sys.argv = ['excel_to_csv.py', args.excel_file]
    if args.output_dir:
        sys.argv.extend(['--output-dir', args.output_dir])
    convert_main()


def load_csv(args):
    """Load CSV data to PostgreSQL."""
    from data_processing.loaders.csv_to_postgres import main as load_main
    sys.argv = ['csv_to_postgres.py', '--data-dir', args.data_dir]
    if args.config:
        sys.argv.extend(['--config', args.config])
    if args.verify_only:
        sys.argv.append('--verify-only')
    load_main()


def main():
    """Main function with subcommands."""
    parser = argparse.ArgumentParser(
        description="UT Data Operations - Data Engineering Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python main.py setup --config config.json
    python main.py convert data/sample.xlsx --output-dir data/
    python main.py load --data-dir data/ --config config.json
    python main.py load --verify-only
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Setup command
    setup_parser = subparsers.add_parser('setup', help='Set up database and tables')
    setup_parser.add_argument('--config', '-c', default='config.json',
                             help='Path to JSON configuration file')
    
    # Convert command
    convert_parser = subparsers.add_parser('convert', help='Convert Excel to CSV')
    convert_parser.add_argument('excel_file', help='Path to Excel file')
    convert_parser.add_argument('--output-dir', '-o', help='Output directory for CSV files')
    
    # Load command
    load_parser = subparsers.add_parser('load', help='Load CSV data to PostgreSQL')
    load_parser.add_argument('--data-dir', '-d', default='data',
                            help='Directory containing CSV files')
    load_parser.add_argument('--config', '-c', help='Path to JSON configuration file')
    load_parser.add_argument('--verify-only', action='store_true',
                            help='Only verify existing data, do not load new data')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        if args.command == 'setup':
            setup_database(args)
        elif args.command == 'convert':
            convert_excel(args)
        elif args.command == 'load':
            load_csv(args)
        else:
            parser.print_help()
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
