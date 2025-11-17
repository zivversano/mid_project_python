import re
import pandas as pd
from typing import List
from models.question_texts import QUESTION_TEXTS


def build_question_metadata(columns: List[str]) -> pd.DataFrame:
    """Build a question metadata dataframe from a list of column names.

    Extracts base question number from columns like: q3, q31, q4r, q21r_2016, q5r_dicho, etc.
    Returns a DataFrame with:
      - question_code: original column name (e.g., 'q4r_dicho')
      - question_number: base integer (e.g., 4)
      - variant: suffix after the base number (e.g., 'r', '_r_2016', '_dicho', or '')
      - question_text: human-readable Hebrew text from QUESTION_TEXTS if available
    """
    data = []
    seen = set()
    for col in columns:
        if not isinstance(col, str):
            continue
        m = re.match(r"^q(\d+)(.*)$", col)
        if not m:
            continue
        num = int(m.group(1))
        variant = m.group(2)  # may be ''
        key = (col, num, variant)
        if key in seen:
            continue
        seen.add(key)
        text = QUESTION_TEXTS.get(num)
        data.append({
            "question_code": col,
            "question_number": num,
            "variant": variant,
            "question_text": text,
        })

    df = pd.DataFrame(data, columns=[
        "question_code", "question_number", "variant", "question_text"
    ])
    return df.sort_values(["question_number", "question_code"]).reset_index(drop=True)
