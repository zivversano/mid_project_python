# Data Engineering Project: Hospital Satisfaction Survey

A Python-based ETL (Extract, Transform, Load) pipeline for processing hospital patient satisfaction survey data. The project reads Excel raw data, transforms it, and loads it into a PostgreSQL database.

## Quick Start

### 1. Install Dependencies

Ensure Python 3.8+ is installed, then set up the project:

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install required packages
pip install -r requirements.txt
```

### 2. Set Up PostgreSQL Database

You have two options for running PostgreSQL:

#### Option A: Docker Container (Recommended for Development)

The easiest way to get started is using the included Docker Compose setup:

```bash
# Start PostgreSQL container (creates 'satisfaction' database automatically)
bash scripts/start_postgres.sh

# The container runs on port 5433 to avoid conflicts with system PostgreSQL
# Credentials: postgres/devpass, database: satisfaction
```

The `.env` file is already configured for the Docker setup:
```
POSTGRES_USER=postgres
POSTGRES_PASSWORD=devpass
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=satisfaction
```

**Container management:**
```bash
# View logs
docker compose logs postgres

# Stop container
docker compose stop postgres

# Start existing container
docker compose start postgres

# Remove container and data
docker compose down -v
```

**Web Interface (pgAdmin):**

Access the database through a web browser:
1. Start pgAdmin: `docker compose up -d pgadmin`
2. Open browser: http://localhost:8080
3. Login credentials:
   - Email: `admin@admin.com`
   - Password: `admin`
4. Add server connection:
   - Right-click "Servers" → Register → Server
   - General tab: Name = `midproj-postgres`
   - Connection tab:
     - Host: `midproj-postgres` (container name)
     - Port: `5432` (internal port)
     - Database: `satisfaction`
     - Username: `postgres`
     - Password: `devpass`
   - Save password: Yes
5. Navigate to: Servers → midproj-postgres → Databases → satisfaction → Schemas → public → Tables

**Query data in pgAdmin:**
- Right-click `satisfaction_2016_cleaned` → View/Edit Data → All Rows
- Or use Query Tool to run SQL

#### Option B: System PostgreSQL

If you prefer using a local PostgreSQL installation:

1. Ensure PostgreSQL is installed and running:
   ```bash
   sudo systemctl status postgresql
   sudo systemctl start postgresql  # if not running
   ```

2. Create the database:
   ```bash
   # Using the init script (requires correct password in .env)
   bash scripts/init_db.sh
   
   # Or manually with sudo
   sudo -u postgres psql -c "CREATE DATABASE satisfaction;"
   sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'yourpassword';"
   ```

3. Update `.env` with your credentials:
   ```
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=yourpassword
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DB=satisfaction
   ```

### 3. Run the ETL Pipeline

```bash
# Using the convenience script (recommended)
./scripts/run_etl.sh

# Or manually with environment variables
export POSTGRES_USER=postgres POSTGRES_PASSWORD=password POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_DB=postgres
python main.py
```

## Project Structure

```
mid_project_python/
├── data/
│   ├── raw/                    # Source Excel files
│   ├── extracted/              # Extracted CSVs and parquet files
│   ├── staging/                # Staging area for intermediate parquets
│   ├── curated/                # Final curated parquets ready for load
│   └── transformed/            # Transformed data with labels
├── extract/                    # Excel extraction scripts
│   ├── extract_excel.py        # Extract sheets → separate xlsx files
│   └── extract_excel_csv.py    # Extract sheets → CSV files
├── repositories/               # ETL step implementations
│   ├── extract_values.py       # Extract values/mapping sheet
│   ├── extract_data.py         # Extract main data sheet
│   ├── extract_info.py         # Extract info sheet
│   ├── transform_clean.py      # Transform & clean data
│   └── load_postgress.py       # Load into PostgreSQL
├── service/
│   └── etl_runner.py           # Orchestrates ETL pipeline
├── transform/
│   └── transform_values_to_labels.py  # Apply label mappings
├── models/
│   └── hospital.py             # Hospital lookup models
├── data_base/
│   └── connection.py           # PostgreSQL connection helper
├── scripts/
│   ├── run_etl.sh              # ETL runner script (recommended)
│   └── run_in_venv.sh          # Generic venv python runner
├── requirements.txt            # Python dependencies
├── .env.example                # Environment template
├── main.py                     # Entry point
└── README.md                   # This file
```

## Database Schema

After running the ETL, the following table is created in PostgreSQL:

**satisfaction_curated** — Hospital satisfaction survey data with ~11k+ rows
- `hospital_name` — Hospital identifier and name lookup
- `code_hospital` — Hospital code (numeric)
- Various survey question scores and flags
- Normalized scores and quality metrics

Query example:
```bash
psql -U postgres -d postgres -c "SELECT COUNT(*) FROM satisfaction_curated;"
```

## ETL Pipeline Steps

1. **Extract**
   - Reads Excel files from `data/raw/`
   - Produces staging parquet files in `data/staging/`
   - Handles multiple sheets (values, data, info)

2. **Transform**
   - Concatenates data from multiple sources
   - Cleans column names (strips whitespace, replaces spaces with underscores)
   - Fills missing values:
     - Numeric columns: filled with mean
     - Categorical columns: filled with mode
   - Removes duplicates
   - Applies hospital name lookups

3. **Load**
   - Writes curated parquet to `data/curated/`
   - Loads into PostgreSQL `satisfaction_curated` table
   - Replaces table if it exists (idempotent)

## Configuration

### Environment Variables

The project reads PostgreSQL credentials from environment variables:

- `POSTGRES_USER` — Database user (default: `postgres`)
- `POSTGRES_PASSWORD` — Database password (default: `password`)
- `POSTGRES_HOST` — Database host (default: `localhost`)
- `POSTGRES_PORT` — Database port (default: `5432`)
- `POSTGRES_DB` — Database name (default: `postgres`)

Set these before running `main.py` or use the `.env` file with `./scripts/run_etl.sh`.

### Virtual Environment

The project uses a Python virtual environment to isolate dependencies. All scripts are designed to run from the project root:

```bash
# Activate the venv
source .venv/bin/activate

# Or use it directly
.venv/bin/python main.py
```

## Requirements

- Python 3.8+
- PostgreSQL 12+ (local or remote)
- Dependencies listed in `requirements.txt`:
  - pandas (data manipulation)
  - openpyxl (read Excel files)
  - pyarrow (read/write Parquet format)
  - python-dateutil (date handling)
  - SQLAlchemy (database abstraction)
  - psycopg2-binary (PostgreSQL driver)

## Common Tasks

### Extract Excel Sheets to Separate Files

Convert source Excel sheets into individual xlsx or CSV files:

```bash
# Extract to separate XLSX files
.venv/bin/python extract/extract_excel.py

# Extract to separate CSV files
.venv/bin/python extract/extract_excel_csv.py
```

### Apply Value Labels to Data

Transform numeric survey responses into human-readable Hebrew labels:

```bash
.venv/bin/python transform/transform_values_to_labels.py
```

### Query Loaded Data

```bash
# Show all tables
psql -U postgres -d postgres -c "\dt"

# Show row count and unique hospitals
psql -U postgres -d postgres -c "SELECT COUNT(*), COUNT(DISTINCT hospital_name) FROM satisfaction_curated;"

# Show sample data
psql -U postgres -d postgres -c "SELECT * FROM satisfaction_curated LIMIT 5;"
```

## Troubleshooting

### ModuleNotFoundError: No module named 'X'

**Cause:** Missing package in the virtual environment.

**Solution:**
```bash
# Activate the venv and install requirements
source .venv/bin/activate
pip install -r requirements.txt

# Or install missing package directly
pip install pandas openpyxl pyarrow sqlalchemy psycopg2-binary
```

### PostgreSQL Connection Refused

**Cause:** PostgreSQL server is not running or connection details are wrong.

**Solution:**

For Docker container (recommended):
```bash
# Start the container
bash scripts/start_postgres.sh

# Check container status
docker ps | grep midproj-postgres

# View logs
docker compose logs postgres

# Test connection
PGPASSWORD=devpass psql -U postgres -h localhost -p 5433 -d satisfaction -c "SELECT 1;"
```

For system PostgreSQL:
```bash
# Check if PostgreSQL is running
sudo systemctl status postgresql

# Start PostgreSQL (Linux)
sudo systemctl start postgresql

# Verify connection
PGPASSWORD=yourpassword psql -U postgres -h localhost -p 5432 -d satisfaction -c "SELECT 1;"
```

### Docker Port Conflict (Address Already in Use)

**Cause:** Port 5432 is already in use by system PostgreSQL.

**Solution:**
The `docker-compose.yml` is configured to use port 5433 on the host to avoid conflicts. Ensure your `.env` has:
```
POSTGRES_PORT=5433
```

If you need to change ports:
1. Edit `docker-compose.yml` ports mapping (e.g., `"5434:5432"`)
2. Update `POSTGRES_PORT` in `.env` to match
3. Restart: `docker compose down && bash scripts/start_postgres.sh`

### ImportError: Missing optional dependency 'openpyxl'

**Cause:** openpyxl (Excel reader) is not installed.

**Solution:**
```bash
.venv/bin/python -m pip install openpyxl
```

### FileNotFoundError: No such file or directory: '...'

**Cause:** Input raw data file is missing or has a different name.

**Solution:**
The main ETL script (`main.py`) automatically discovers raw files in `data/raw/` by pattern matching:
- `*values*.xlsx` → values/mapping sheet
- `*data*.xlsx` → main data sheet
- `*info*.xlsx` or `*_2_*.xlsx` → info sheet

Ensure your raw files are named to match these patterns, or update the file paths in `main.py`.

### Parquet Conversion Error

**Cause:** Mixed data types in columns (e.g., strings in a numeric column).

**Solution:**
- Ensure raw Excel files are clean before running ETL
- The transform step attempts to clean and fill missing values
- Check `data/staging/` and `data/curated/` for intermediate parquet files

## Development

### Running Tests

(Test framework can be added; currently tests are manual via running the pipeline.)

### Code Style

- Follow PEP 8 conventions
- Use type hints where practical
- Add docstrings to functions and modules

### Adding New Steps to the ETL

1. Create a new module in `repositories/` with a `run_*` function
2. Import and call it from `service/etl_runner.py`
3. Update `main.py` if you need to expose new configuration

## Contributing

When contributing:
1. Ensure the venv is activated and all dependencies are installed
2. Test changes by running `./scripts/run_etl.sh`
3. Update this README if you add new steps or configuration
4. Commit changes to git

## License

(Add your license here if applicable)

## Support

For issues or questions:
1. Check the **Troubleshooting** section above
2. Review logs in `data/staging/` and `data/curated/`
3. Verify PostgreSQL is running and accessible
4. Ensure environment variables are correctly set in `.env`

---

**Last Updated:** November 15, 2025  
**Python Version:** 3.8+  
**PostgreSQL Version:** 12+
