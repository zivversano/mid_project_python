from service.etl_runner import run_etl
from pathlib import Path
import sys


def _find_file(pattern: str) -> str | None:
    files = list(Path("data/raw").glob(pattern))
    return str(files[0]) if files else None


if __name__ == "__main__":
    # Automatically locate the necessary raw Excel files instead of hardcoding
    values_file = _find_file("*values*.xlsx")
    data_file = _find_file("*data*.xlsx")
    info_file = _find_file("*info*.xlsx") or _find_file("*_2_*.xlsx")

    missing = [name for name, path in (
        ("values", values_file), ("data", data_file), ("info", info_file)
    ) if path is None]
    if missing:
        print(f"Error: could not find required raw files for: {', '.join(missing)} in data/raw")
        print("Available files:")
        for p in sorted(Path("data/raw").glob("*.xlsx")):
            print(" -", p.name)
        sys.exit(1)

    # Run the ETL pipeline
    run_etl(values_file, data_file, info_file)
