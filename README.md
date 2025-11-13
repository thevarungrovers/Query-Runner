# Query Runner

A Python script that executes SQL queries from a file against a MySQL database and saves the results to CSV files.

## Features

- ✅ **Auto-detect SQL files** - No need to specify filename when running
- ✅ **Environment-based credentials** - Secure database configuration via `.env` file
- ✅ **Multiple queries support** - Execute single or multiple SQL queries from one file
- ✅ **Automatic CSV generation** - Each query result saved to a timestamped CSV file
- ✅ **Complex query support** - Works with JOINs, aggregations, and subqueries
- ✅ **Clear progress reporting** - Real-time feedback on query execution
- ✅ **Error handling** - Detailed error messages for troubleshooting
- ✅ **Configurable output** - Customize output directory location

## Quick Start

1. **Install Python dependencies:**

```bash
pip install -r requirements.txt
```

2. **Create your `.env` file with database credentials:**

Create a file named `.env` in the project directory:

```env
DB_HOST=localhost
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_NAME=your_database_name
DB_PORT=3306
```

3. **Create your SQL file** (e.g., `queries.sql`):

```sql
SELECT * FROM users LIMIT 10;
SELECT COUNT(*) as total FROM orders;
```

4. **Run the script:**

```bash
python query_runner.py
```

That's it! Check the `output/` directory for your CSV files.

## Usage

### Basic Usage

The simplest way to run the script - it will auto-detect your SQL file:

```bash
python query_runner.py
```

**How it works:**
- Automatically finds `.sql` files in the current directory
- Uses the first one it finds (alphabetically)
- If multiple SQL files exist, displays a list and uses the first one
- You can override this with the `-f` option

### Advanced Options

**Specify a specific SQL file:**

```bash
python query_runner.py -f reports.sql
```

**Use a custom output directory:**

```bash
python query_runner.py -o results/
```

**Combine options:**

```bash
python query_runner.py -f queries.sql -o results/
```

### Command Line Arguments

| Argument | Short | Description | Default |
|----------|-------|-------------|---------|
| `--file` | `-f` | Path to SQL file (optional, auto-detects if not provided) | First `.sql` file found |
| `--output-dir` | `-o` | Output directory for CSV files | `output/` |

### Environment Variables

Configure these in your `.env` file:

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `DB_HOST` | No | Database host | `localhost` |
| `DB_USER` | **Yes** | Database username | - |
| `DB_PASSWORD` | **Yes** | Database password | - |
| `DB_NAME` | **Yes** | Database name | - |
| `DB_PORT` | No | Database port | `3306` |

## SQL File Format

### Syntax Rules

- **Multiple queries**: Separate with semicolons (`;`)
- **Comments**: Use `--` for single-line comments
- **Complex queries**: JOINs, subqueries, and CTEs are fully supported
- **Whitespace**: Flexible formatting - use newlines and indentation as needed

### Example SQL File

**Simple queries:**

```sql
-- Query 1: Get all active users
SELECT * FROM users WHERE status = 'active';

-- Query 2: Count total orders
SELECT COUNT(*) as total_orders FROM orders;
```

**Complex queries:**

```sql
-- Get order summary with customer information
SELECT 
    DATE(o.order_date) as date,
    COUNT(DISTINCT o.order_id) as total_orders,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    SUM(o.amount) as total_revenue,
    AVG(o.amount) as avg_order_value
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY DATE(o.order_date)
ORDER BY date DESC;

-- Top 10 products by revenue
SELECT 
    p.product_name,
    SUM(oi.quantity) as units_sold,
    SUM(oi.quantity * oi.unit_price) as total_revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_revenue DESC
LIMIT 10;
```

## Output Format

### File Naming

Each query generates a separate CSV file with a timestamp:

```
output/
├── query_1_20251113_105340.csv
├── query_2_20251113_105340.csv
└── query_3_20251113_105340.csv
```

**Format:** `query_[N]_[YYYYMMDD]_[HHMMSS].csv`
- `N`: Query number (1, 2, 3, etc.)
- `YYYYMMDD`: Date (Year-Month-Day)
- `HHMMSS`: Time (Hour-Minute-Second)

### CSV Structure

- **Headers**: Column names from SQL query
- **Encoding**: UTF-8
- **Delimiter**: Comma (`,`)
- **Line endings**: System default

### Example Output

For the query: `SELECT user_id, username, email FROM users LIMIT 3;`

**Output file:** `query_1_20251113_105340.csv`

```csv
user_id,username,email
1,john_doe,john@example.com
2,jane_smith,jane@example.com
3,bob_jones,bob@example.com
```

## Complete Example

### Step 1: Setup Environment

Create `.env`:

```env
DB_HOST=localhost
DB_USER=myuser
DB_PASSWORD=mypassword
DB_NAME=sales_db
DB_PORT=3306
```

### Step 2: Create SQL File

Create `queries.sql`:

```sql
-- Daily sales summary
SELECT 
    DATE(order_date) as date,
    COUNT(*) as total_orders,
    SUM(total_amount) as revenue
FROM orders
WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
GROUP BY DATE(order_date)
ORDER BY date DESC;

-- Top customers
SELECT 
    customer_id,
    customer_name,
    COUNT(*) as order_count,
    SUM(total_amount) as lifetime_value
FROM orders
GROUP BY customer_id, customer_name
ORDER BY lifetime_value DESC
LIMIT 10;
```

### Step 3: Run the Script

```bash
python query_runner.py
```

### Step 4: Output

```
============================================================
Query Runner - 2025-11-13 10:53:40
============================================================

Reading SQL file: queries.sql
✓ Found 2 query/queries

✓ Connected to database: sales_db

Executing queries...

[1/2] Executing query...
✓ Query executed successfully: 7 rows written to output/query_1_20251113_105340.csv

[2/2] Executing query...
✓ Query executed successfully: 10 rows written to output/query_2_20251113_105340.csv

============================================================
Summary: 2/2 queries executed successfully
Output directory: /path/to/output
============================================================
```

## Error Handling

The script provides clear, actionable error messages:

### Common Errors

**No SQL file found:**
```
✗ Error: No SQL files found in current directory
  Please create a .sql file or specify one with -f option
```

**Missing environment variables:**
```
✗ Error: DB_USER environment variable is required
```

**Database connection failed:**
```
✗ Error connecting to database: Access denied for user 'root'@'localhost'
```

**Query execution failed:**
```
✗ Error executing query: Table 'mydb.invalid_table' doesn't exist
  Query: SELECT * FROM invalid_table...
```

### Validation

The script validates:
- ✅ SQL file exists and is readable
- ✅ Required environment variables are set
- ✅ Database connection is successful
- ✅ Output directory can be created/written to

### Summary Report

After execution, you'll see a summary:
```
============================================================
Summary: 2/3 queries executed successfully
Output directory: /path/to/output
============================================================
```

## Requirements

- **Python**: 3.6 or higher
- **MySQL**: 5.7 or higher (or compatible database)
- **Dependencies**: Listed in `requirements.txt`
  - `mysql-connector-python` - MySQL database driver
  - `python-dotenv` - Environment variable management

## Troubleshooting

### Import Error: No module named 'mysql.connector'

```bash
pip install -r requirements.txt
```

### Permission Denied on Output Directory

Ensure you have write permissions in the current directory or specify a different output directory:

```bash
python query_runner.py -o ~/my_results/
```

### Large Result Sets

For very large queries (millions of rows), consider:
- Adding `LIMIT` clauses to your queries
- Running queries in batches
- Increasing available memory

### Connection Timeout

If connecting to a remote database, ensure:
- Database server is accessible from your network
- Firewall rules allow MySQL connections (port 3306)
- Database user has appropriate permissions

## Best Practices

1. **Use specific queries**: Avoid `SELECT *` on large tables
2. **Add LIMIT clauses**: For exploratory queries, limit results
3. **Test queries first**: Run complex queries in your MySQL client first
4. **Version control**: Keep SQL files in version control (but not `.env`!)
5. **Backup data**: Always backup before running UPDATE/DELETE queries
6. **Use comments**: Document what each query does in your SQL file

## Security Notes

⚠️ **Important:**
- Never commit your `.env` file to version control
- The `.gitignore` file already excludes `.env`
- Use strong passwords for database access
- Limit database user permissions to only what's needed (SELECT for read-only queries)
- Be cautious with queries that modify data (UPDATE, DELETE, INSERT)

