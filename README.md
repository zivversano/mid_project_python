# Hospital Satisfaction ETL

Python ETL that reads the raw Excel into Parquet, cleans/transforms, and loads to PostgreSQL with a readable view and helpful CSVs.

## Setup

### 1) Create venv and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
# install via main.py helper (uses the current interpreter)
python main.py --install-deps
# or traditional
pip install -r requirements.txt
```

### 2) Start PostgreSQL (Docker)

```bash
# starts postgres on localhost:5433 and pgAdmin on localhost:8081
bash scripts/start_postgres.sh
```

.env is preconfigured for Docker:
```
POSTGRES_USER=postgres
POSTGRES_PASSWORD=devpass
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=satisfaction
```

pgAdmin: http://localhost:8081  (admin@admin.com / admin)

## Run

```bash
# run the full pipeline
python main.py
```

### Launch Dashboard

After running the ETL, visualize the data:
```bash
streamlit run dashboard.py
```

This opens an interactive dashboard at http://localhost:8501 with:
- Overview: Key metrics and distribution charts
- Hospital Comparison: Compare up to 5 hospitals side-by-side
- Question Analysis: Deep dive into individual survey questions
- Data Explorer: Browse and filter the raw data

What the ETL does:
- Extracts: reads data/raw/satisfaction_2016_data_*.xlsx → data/output/satisfaction_2016_data.parquet
- Transforms: cleans, applies mappings; saves data/output/cleaned_data.parquet
- Aggregates: saves data/output/hospital_scores.csv
- Metadata: saves data/output/question_texts.parquet
- PostgreSQL load: creates tables
  - satisfaction_2016_cleaned
  - hospital_scores
  - question_texts
- View: creates vw_satisfaction_readable (Hebrew aliases for q* columns)
- Convenience CSV: data/output/cleaned_data_readable_headers.csv (Hebrew column headers)

## Querying in Postgres

Connect:
```bash
psql -h localhost -p 5433 -U postgres -d satisfaction
```

Quick checks:
```sql
-- confirm tables
SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;

-- confirm view
SELECT table_name FROM information_schema.views WHERE table_schema='public' AND table_name='vw_satisfaction_readable';

-- sample rows
SELECT * FROM satisfaction_2016_cleaned LIMIT 5;
SELECT * FROM hospital_scores ORDER BY overall_average DESC LIMIT 10;
SELECT * FROM vw_satisfaction_readable LIMIT 5;
```

There is also a ready-made script with more queries:
```bash
psql -h localhost -p 5433 -U postgres -d satisfaction -f scripts/query_postgres.sql
```

## Notes

- If you get “relation vw_satisfaction_readable does not exist”, ensure you’re connected to DB "satisfaction" on port 5433, then re-run: `python -c "from repositories.postgres_views import create_readable_view; create_readable_view()"`.
- Hebrew readable CSV isn’t loaded into Postgres due to identifier length constraints; use the view or the CSV file directly.
- You can regenerate hospital scores alone from the CSV with readable headers if needed.

## Troubleshooting

- Ensure Docker Postgres is up: `docker ps | grep postgres` and `docker compose logs postgres`
- Test DB connection: `PGPASSWORD=devpass psql -U postgres -h localhost -p 5433 -d satisfaction -c "SELECT 1;"`
- Install dependencies again if imports fail: `python main.py --install-deps`

---

Last updated: 2025-11-17
