import re
from typing import List, Dict
from models.question_texts import QUESTION_TEXTS
from .load_postgress import get_postgres_engine


def _escape_ident(ident: str) -> str:
    """Escape a SQL identifier with double quotes and escape any embedded quotes."""
    return '"' + ident.replace('"', '""') + '"'


def _alias_for_column(col: str, question_texts: Dict[int, str]) -> str:
    """Return a human-readable alias for q* columns; otherwise return original name.

    The alias format is: q{num}__{short_text}
    Short text is trimmed to ~80 chars to keep headers manageable.
    """
    m = re.match(r"^q(\d+)(.*)$", col)
    if not m:
        return col
    num = int(m.group(1))
    suffix = m.group(2) or ""
    text = question_texts.get(num)
    if not text:
        return col
    short = text.replace('\n', ' ').strip()
    if len(short) > 80:
        short = short[:77] + '...'
    # keep suffix to disambiguate derived columns (e.g., q3, q3_g, q3_5down)
    alias_core = f"q{num}{suffix}"
    return f"{alias_core}__{short}"


def create_readable_view(
    source_table: str = "satisfaction_2016_cleaned",
    view_name: str = "vw_satisfaction_readable",
):
    """Create or replace a Postgres VIEW that aliases q* columns with human-readable names.

    The base table remains unchanged; the view presents readable headers.
    """
    engine = get_postgres_engine()
    # Use a transactional block to ensure DDL is committed
    with engine.begin() as conn:
        # fetch existing column names in order
        # Build a safe literal for the table name (controlled by our code)
        cols_sql = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = '{source_table}'
            ORDER BY ordinal_position
        """
        cols = conn.exec_driver_sql(cols_sql).scalars().all()

        select_parts: List[str] = []
        for c in cols:
            alias = _alias_for_column(c, QUESTION_TEXTS)
            if alias == c:
                select_parts.append(_escape_ident(c))
            else:
                select_parts.append(f"{_escape_ident(c)} AS {_escape_ident(alias)}")

        select_sql = ",\n    ".join(select_parts)
        ddl = f"""
        DROP VIEW IF EXISTS {_escape_ident(view_name)} CASCADE;
        CREATE VIEW {_escape_ident(view_name)} AS
        SELECT
            {select_sql}
        FROM {_escape_ident(source_table)};
        """
        conn.exec_driver_sql(ddl)

    # Separate connection to confirm visibility after commit
    with engine.connect() as verify_conn:
        verify_sql = f"""
            SELECT count(*)
            FROM information_schema.views
            WHERE table_schema='public' AND table_name = '{view_name}'
        """
        count = verify_conn.exec_driver_sql(verify_sql).scalar_one_or_none()
        print(f"Created/updated view '{view_name}' with {len(cols)} columns; visible: {bool(count)}")
