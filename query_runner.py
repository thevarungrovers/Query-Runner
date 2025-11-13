#!/usr/bin/env python3
"""
Query Runner - Execute SQL queries from a file and save results to CSV
"""

import mysql.connector
import csv
import os
import sys
from datetime import datetime
from pathlib import Path
import argparse
from dotenv import load_dotenv


def parse_sql_file(sql_file_path):
    """
    Parse SQL file and extract individual queries.
    Supports multiple queries separated by semicolons.
    
    Args:
        sql_file_path: Path to the SQL file
        
    Returns:
        List of SQL query strings
    """
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split by semicolon and filter out empty queries
    queries = [q.strip() for q in content.split(';') if q.strip()]
    
    return queries


def connect_to_database(host, user, password, database, port=3306):
    """
    Establish connection to MySQL database.
    
    Args:
        host: Database host
        user: Database user
        password: Database password
        database: Database name
        port: Database port (default: 3306)
        
    Returns:
        MySQL connection object
    """
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )
        print(f"✓ Connected to database: {database}")
        return connection
    except mysql.connector.Error as err:
        print(f"✗ Error connecting to database: {err}")
        sys.exit(1)


def execute_query_to_csv(connection, query, output_file):
    """
    Execute a SQL query and save results to CSV file.
    
    Args:
        connection: MySQL connection object
        query: SQL query string
        output_file: Path to output CSV file
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        
        # Fetch all results
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        
        # Write to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(column_names)  # Write header
            csv_writer.writerows(results)  # Write data
        
        row_count = len(results)
        print(f"✓ Query executed successfully: {row_count} rows written to {output_file}")
        
        cursor.close()
        return True
        
    except mysql.connector.Error as err:
        print(f"✗ Error executing query: {err}")
        print(f"  Query: {query[:100]}...")
        return False


def find_sql_files(directory='.'):
    """
    Find all SQL files in the given directory.
    
    Args:
        directory: Directory to search for SQL files
        
    Returns:
        List of SQL file paths
    """
    sql_files = list(Path(directory).glob('*.sql'))
    return sorted(sql_files)


def main():
    # Load environment variables from .env file
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description='Execute SQL queries from a file and save results to CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables (required):
  DB_HOST       Database host (default: localhost)
  DB_USER       Database user
  DB_PASSWORD   Database password
  DB_NAME       Database name
  DB_PORT       Database port (default: 3306)

Examples:
  python query_runner.py                    # Finds and runs first .sql file in current directory
  python query_runner.py -f queries.sql     # Runs specific SQL file
  python query_runner.py -o results/        # Custom output directory
        """
    )
    
    parser.add_argument('-f', '--file', help='Path to SQL file (optional, auto-detects if not provided)')
    parser.add_argument('-o', '--output-dir', default='output', help='Output directory for CSV files (default: output)')
    
    args = parser.parse_args()
    
    # Determine which SQL file to use
    if args.file:
        sql_file = args.file
    else:
        # Auto-detect SQL files in current directory
        sql_files = find_sql_files()
        if not sql_files:
            print("✗ Error: No SQL files found in current directory")
            print("  Please create a .sql file or specify one with -f option")
            sys.exit(1)
        elif len(sql_files) > 1:
            print(f"Found {len(sql_files)} SQL files:")
            for idx, f in enumerate(sql_files, 1):
                print(f"  {idx}. {f.name}")
            print(f"\nUsing: {sql_files[0].name}")
            print("  (Use -f option to specify a different file)\n")
        sql_file = str(sql_files[0])
    
    # Get database credentials from environment variables
    db_host = os.getenv('DB_HOST', 'localhost')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    db_port = int(os.getenv('DB_PORT', '3306'))
    
    # Validate required environment variables
    if not db_user:
        print("✗ Error: DB_USER environment variable is required")
        sys.exit(1)
    if not db_password:
        print("✗ Error: DB_PASSWORD environment variable is required")
        sys.exit(1)
    if not db_name:
        print("✗ Error: DB_NAME environment variable is required")
        sys.exit(1)
    
    # Validate SQL file exists
    if not os.path.isfile(sql_file):
        print(f"✗ Error: SQL file not found: {sql_file}")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Query Runner - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # Parse SQL file
    print(f"Reading SQL file: {sql_file}")
    queries = parse_sql_file(sql_file)
    print(f"✓ Found {len(queries)} query/queries\n")
    
    # Connect to database
    connection = connect_to_database(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        port=db_port
    )
    
    print(f"\nExecuting queries...\n")
    
    # Execute each query and save to CSV
    success_count = 0
    for idx, query in enumerate(queries, 1):
        # Generate output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = output_dir / f"query_{idx}_{timestamp}.csv"
        
        print(f"[{idx}/{len(queries)}] Executing query...")
        if execute_query_to_csv(connection, query, output_file):
            success_count += 1
        print()
    
    # Close connection
    connection.close()
    
    # Summary
    print(f"{'='*60}")
    print(f"Summary: {success_count}/{len(queries)} queries executed successfully")
    print(f"Output directory: {output_dir.absolute()}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()

