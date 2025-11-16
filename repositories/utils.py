import pandas as pd
import yaml
import os
from dotenv import load_dotenv


def load_env():
    """Load environment variables related to PostgreSQL connection."""
    load_dotenv()
    return {
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
    }


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize and de-duplicate column names.

    Steps:
    1. Trim whitespace, lowercase, replace internal whitespace with underscores.
    2. Remove non-alphanumeric/underscore characters.
    3. Convert blank or underscore-only names to 'unnamed'.
    4. Ensure uniqueness by appending '__{n}' where duplicates occur.

    Returns a new DataFrame with updated columns.
    """
    # Basic normalization
    normalized = (
        df.columns.str.strip()  # remove leading/trailing whitespace
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)  # replace spaces with underscores
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)  # remove special characters
    )

    # Handle empty or underscore-only names
    processed = []
    counts = {}
    for col in normalized:
        base = col
        if base == "" or base.strip("_") == "":
            base = "unnamed"
        # Ensure uniqueness
        if base in counts:
            counts[base] += 1
            unique = f"{base}__{counts[base]}"
        else:
            counts[base] = 0
            unique = base
        processed.append(unique)

    # Optional: Inform about duplicates resolved
    duplicates_resolved = [b for b, c in counts.items() if c > 0]
    if duplicates_resolved:
        print(f"[normalize_columns] Resolved duplicates for: {duplicates_resolved}")

    df = df.copy()
    df.columns = processed
    return df
