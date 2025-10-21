#!/usr/bin/env python3
"""
Excel to CSV Converter Script

This script converts all sheets from an Excel file into individual CSV files.
Each CSV file will be named after the corresponding sheet name.

Usage:
    python excel_to_csv_converter.py <excel_file_path> [output_directory]

Requirements:
    - pandas
    - openpyxl (for .xlsx files)
    - xlrd (for .xls files)
"""

import pandas as pd
import os
import sys
import argparse
from pathlib import Path


def convert_excel_to_csv(excel_file_path, output_dir=None):
    """
    Convert all sheets from an Excel file to individual CSV files.
    
    Args:
        excel_file_path (str): Path to the Excel file
        output_dir (str, optional): Directory to save CSV files. 
                                  If None, saves in the same directory as Excel file.
    
    Returns:
        list: List of created CSV file paths
    """
    excel_path = Path(excel_file_path)
    
    # Validate input file
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_file_path}")
    
    if not excel_path.suffix.lower() in ['.xlsx', '.xls']:
        raise ValueError(f"Unsupported file format: {excel_path.suffix}. Only .xlsx and .xls are supported.")
    
    # Set output directory
    if output_dir is None:
        output_dir = excel_path.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Read Excel file and get sheet names
    try:
        excel_file = pd.ExcelFile(excel_file_path)
        sheet_names = excel_file.sheet_names
        print(f"Found {len(sheet_names)} sheets: {', '.join(sheet_names)}")
    except Exception as e:
        raise Exception(f"Error reading Excel file: {str(e)}")
    
    created_files = []
    
    # Convert each sheet to CSV
    for sheet_name in sheet_names:
        try:
            print(f"Converting sheet: {sheet_name}")
            
            # Read the sheet without header, skipping the first row (table name)
            df = pd.read_excel(excel_file_path, sheet_name=sheet_name, header=None, skiprows=1)
            
            # Clean sheet name for filename (remove invalid characters)
            clean_sheet_name = "".join(c for c in sheet_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            clean_sheet_name = clean_sheet_name.replace(' ', '_')
            
            # Create CSV filename
            csv_filename = f"{clean_sheet_name}.csv"
            csv_path = output_dir / csv_filename
            
            # Save as CSV without header
            df.to_csv(csv_path, index=False, header=False, encoding='utf-8')
            created_files.append(str(csv_path))
            
            print(f"  ✓ Created: {csv_path}")
            print(f"  ✓ Rows: {len(df)}, Columns: {len(df.columns)}")
            
        except Exception as e:
            print(f"  ✗ Error converting sheet '{sheet_name}': {str(e)}")
            continue
    
    return created_files


def main():
    """Main function to handle command line arguments and execute conversion."""
    parser = argparse.ArgumentParser(
        description="Convert all sheets from an Excel file to individual CSV files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python excel_to_csv_converter.py data/sample.xlsx
    python excel_to_csv_converter.py data/sample.xlsx output/
    python excel_to_csv_converter.py data/sample.xlsx --output-dir results/
        """
    )
    
    parser.add_argument('excel_file', help='Path to the Excel file to convert')
    parser.add_argument('-o', '--output-dir', 
                       help='Output directory for CSV files (default: same as Excel file)')
    parser.add_argument('--version', action='version', version='Excel to CSV Converter 1.0')
    
    args = parser.parse_args()
    
    try:
        print("Excel to CSV Converter")
        print("=" * 50)
        print(f"Input file: {args.excel_file}")
        print(f"Output directory: {args.output_dir or 'Same as input file'}")
        print()
        
        # Convert Excel to CSV
        created_files = convert_excel_to_csv(args.excel_file, args.output_dir)
        
        print()
        print("Conversion Summary:")
        print("=" * 50)
        print(f"✓ Successfully converted {len(created_files)} sheets")
        print("Created files:")
        for file_path in created_files:
            print(f"  - {file_path}")
        
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
