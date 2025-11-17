import os
import re
from typing import List, Tuple

import numpy as np
import pandas as pd


def _select_question_columns(columns: List[str]) -> List[str]:
    """Return columns that look like question columns (e.g., q3, q3_g, q21r_2016).

    Pattern: starts with 'q' followed by a digit. Suffixes like _g, _dicho are allowed.
    """
    pattern = re.compile(r"^q\d")
    return [c for c in columns if pattern.match(c)]


def compute_hospital_scores(df: pd.DataFrame, hospital_col: str = "code_hospital") -> pd.DataFrame:
    """Compute per-hospital averages for each question and an overall average.

    Contract:
    - Input: DataFrame containing a hospital code column and question columns named like q<digits>[_suffix].
    - Output: DataFrame indexed by hospital with one column per question mean and an 'overall_average' column.
    - Non-numeric question values are coerced to NaN; means are computed ignoring NaN.

    Edge cases handled:
    - Missing hospital codes: rows with NaN in hospital_col are dropped for grouping.
    - Non-numeric question entries: coerced to NaN.
    - Hospitals with no valid responses in some questions: mean is NaN for those questions.
    - Overall average is weighted across all available question responses (sum/count), not mean-of-means.
    """
    if hospital_col not in df.columns:
        raise KeyError(f"Hospital column '{hospital_col}' not found in DataFrame")

    qcols = _select_question_columns(list(df.columns))
    if not qcols:
        raise ValueError("No question columns found (expected names starting with 'q<digit>')")

    work = df.copy()

    # Coerce question columns to numeric, preserving NaN where non-numeric
    work[qcols] = work[qcols].apply(pd.to_numeric, errors="coerce")

    # Drop rows with missing hospital code for grouping
    work = work.dropna(subset=[hospital_col])

    # Group by hospital
    g = work.groupby(hospital_col, dropna=False)

    # Per-question means
    means = g[qcols].mean()

    # Overall weighted average across all question responses per hospital
    sums = g[qcols].sum(min_count=1)
    counts = g[qcols].count()
    total_sum = sums.sum(axis=1)
    total_count = counts.sum(axis=1)
    overall = (total_sum / total_count).rename("overall_average")

    result = means.join(overall)
    result = result.reset_index()  # bring hospital code back as a column

    return result


def save_hospital_scores_csv(result_df: pd.DataFrame, output_path: str) -> str:
    """Save the hospital scores DataFrame to CSV, ensuring the directory exists."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    result_df.to_csv(output_path, index=False)
    return output_path
