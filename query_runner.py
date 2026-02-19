#!/usr/bin/env python3
"""
Query Runner - Execute SQL queries from a file and save results to CSV
"""

import mysql.connector
import csv
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import argparse
from dotenv import load_dotenv
import re


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


def extract_date_range(queries):
    """
    Extract start and end dates from SET statements in queries.
    
    Args:
        queries: List of SQL query strings
        
    Returns:
        tuple: (start_date, end_date) as datetime objects, or (None, None) if not found
    """
    start_date = None
    end_date = None
    
    for query in queries:
        # Match SET @start_date = "YYYY-MM-DD"
        start_match = re.search(r'SET\s+@start_date\s*=\s*["\'](\d{4}-\d{2}-\d{2})["\']', query, re.IGNORECASE)
        if start_match:
            start_date = datetime.strptime(start_match.group(1), '%Y-%m-%d')
        
        # Match SET @end_date = "YYYY-MM-DD"
        end_match = re.search(r'SET\s+@end_date\s*=\s*["\'](\d{4}-\d{2}-\d{2})["\']', query, re.IGNORECASE)
        if end_match:
            end_date = datetime.strptime(end_match.group(1), '%Y-%m-%d')
    
    return start_date, end_date


def generate_daily_ranges(start_date, end_date):
    """
    Generate list of daily date ranges between start and end dates.
    
    Args:
        start_date: datetime object for start date
        end_date: datetime object for end date
        
    Returns:
        List of tuples [(date1, date1), (date2, date2), ...]
    """
    date_ranges = []
    current_date = start_date
    
    while current_date <= end_date:
        date_ranges.append((current_date, current_date))
        current_date += timedelta(days=1)
    
    return date_ranges


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
        
        # Check if the query returns results (cursor.description is None for non-SELECT queries)
        if cursor.description is None:
            # Query doesn't return results (e.g., SET, UPDATE, INSERT, DELETE)
            cursor.close()
            print(f"✓ Query executed successfully (no results to write)")
            return True
        
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


def execute_query_daily_to_csv(connection, queries, output_file):
    """
    Execute queries day by day and combine results into a single CSV file.
    
    Args:
        connection: MySQL connection object
        queries: List of SQL query strings
        output_file: Path to output CSV file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Extract date range
        start_date, end_date = extract_date_range(queries)
        
        if not start_date or not end_date:
            print("✗ Could not extract date range from queries")
            return False
        
        # Generate daily ranges
        date_ranges = generate_daily_ranges(start_date, end_date)
        total_days = len(date_ranges)
        
        print(f"  Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"  Processing {total_days} days...")
        
        cursor = connection.cursor()
        all_results = []
        column_names = None
        
        # Process each day
        for idx, (day_start, day_end) in enumerate(date_ranges, 1):
            # Execute SET statements with daily dates
            for query in queries:
                if query.strip().upper().startswith('SET'):
                    # Replace date values with current day
                    modified_query = query
                    if '@start_date' in query.lower():
                        modified_query = re.sub(
                            r'(SET\s+@start_date\s*=\s*)["\'][\d-]+["\']',
                            f'\\1"{day_start.strftime("%Y-%m-%d")}"',
                            modified_query,
                            flags=re.IGNORECASE
                        )
                    if '@end_date' in query.lower():
                        modified_query = re.sub(
                            r'(SET\s+@end_date\s*=\s*)["\'][\d-]+["\']',
                            f'\\1"{day_end.strftime("%Y-%m-%d")}"',
                            modified_query,
                            flags=re.IGNORECASE
                        )
                    cursor.execute(modified_query)
                else:
                    # Execute SELECT query
                    cursor.execute(query)
                    
                    # Get column names from first execution
                    if column_names is None and cursor.description:
                        column_names = [desc[0] for desc in cursor.description]
                    
                    # Fetch results if any
                    if cursor.description:
                        results = cursor.fetchall()
                        all_results.extend(results)
            
            # Progress indicator
            if idx % 10 == 0 or idx == total_days:
                print(f"  Progress: {idx}/{total_days} days processed ({len(all_results)} rows so far)")
        
        # Write all results to CSV
        if column_names:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow(column_names)  # Write header
                csv_writer.writerows(all_results)  # Write all data
            
            print(f"✓ Query executed successfully: {len(all_results)} total rows written to {output_file}")
        else:
            print(f"✓ Query executed successfully (no results to write)")
        
        cursor.close()
        return True
        
    except mysql.connector.Error as err:
        print(f"✗ Error executing query: {err}")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
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


def append_run_log(log_file, run_date, start_date, end_date, total_sec, status, sql_query):
    """
    Append a row to the run log CSV file.

    Args:
        log_file: Path to the log CSV file
        run_date: Date of the run (YYYY-MM-DD)
        start_date: Run start datetime string
        end_date: Run end datetime string
        total_sec: Total execution time in seconds
        status: success or failure
        sql_query: The SQL query/queries content
    """
    file_exists = log_file.exists()
    with open(log_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['date', 'start date', 'end date', 'total time(sec)', 'status', 'sql query'])
        writer.writerow([run_date, start_date, end_date, total_sec, status, sql_query])


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

    start_time = datetime.now()
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    # Check if queries contain date range for daily processing
    start_date, end_date = extract_date_range(queries)

    # Execute queries
    success_count = 0
    status = "success"

    if start_date and end_date:
        # Use day-by-day processing for queries with date ranges
        print(f"Detected date range in queries. Using day-by-day processing for optimal performance.\n")
        
        # Generate output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = output_dir / f"combined_results_{timestamp}.csv"
        
        print(f"[1/1] Executing query with daily processing...")
        if execute_query_daily_to_csv(connection, queries, output_file):
            success_count += 1
        else:
            status = "failure"
        print()
    else:
        # Execute each query normally
        for idx, query in enumerate(queries, 1):
            # Generate output filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = output_dir / f"query_{idx}_{timestamp}.csv"
            
            print(f"[{idx}/{len(queries)}] Executing query...")
            if execute_query_to_csv(connection, query, output_file):
                success_count += 1
            print()
        if success_count < len(queries):
            status = "failure"

    # Close connection
    connection.close()

    # Write run log to separate logs folder
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    end_time = datetime.now()
    total_sec = (end_time - start_time).total_seconds()
    log_file = log_dir / "query_runner_log.csv"
    append_run_log(
        log_file,
        run_date=start_time.strftime('%Y-%m-%d'),
        start_date=start_time.strftime('%Y-%m-%d %H:%M:%S'),
        end_date=end_time.strftime('%Y-%m-%d %H:%M:%S'),
        total_sec=round(total_sec, 2),
        status=status,
        sql_query=sql_content
    )

    # Summary
    print(f"{'='*60}")
    if start_date and end_date:
        print(f"Summary: Query executed successfully with day-by-day processing")
    else:
        print(f"Summary: {success_count}/{len(queries)} queries executed successfully")
    print(f"Output directory: {output_dir.absolute()}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()

