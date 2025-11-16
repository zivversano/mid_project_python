"""Postgres connection helper used by repository code.

Provides a single function `get_postgres_engine()` which reads connection
parameters from environment variables and returns a SQLAlchemy engine.
"""

import os

try:
	from sqlalchemy import create_engine
except ModuleNotFoundError:
	raise ModuleNotFoundError(
		"Missing dependency 'SQLAlchemy'. Install it into the project venv:\n"
		"  .venv/bin/python -m pip install SQLAlchemy psycopg2-binary\n"
		"Then run your script with the venv python."
	)


def get_postgres_engine():
	"""Return a SQLAlchemy engine for PostgreSQL using environment variables.

	Environment variables (with defaults):
	  - POSTGRES_USER (user)
	  - POSTGRES_PASSWORD (ziv2405!)
	  - POSTGRES_HOST (localhost)
	  - POSTGRES_PORT (5432)
	  - POSTGRES_DB (postgres)
	"""
	user = os.getenv("POSTGRES_USER", "user")
	password = os.getenv("POSTGRES_PASSWORD", "ziv2405!")
	host = os.getenv("POSTGRES_HOST", "localhost")
	port = os.getenv("POSTGRES_PORT", "5432")
	db = os.getenv("POSTGRES_DB", "postgres")

	url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
	return create_engine(url)

