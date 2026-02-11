#!/usr/bin/env python3
import csv
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple


logger = logging.getLogger(__name__)


# ============================
# Config
# ============================
class Config:
    BASE_DIR = Path(__file__).resolve().parents[1]
    SQL_DIR = BASE_DIR / "sql_param"
    CSV_PATH = BASE_DIR / "src" / "list" / "bq.csv"
    JSON_PATH = BASE_DIR / "src" / "list" / "bq.json"


# ============================
# Logging
# ============================
class _MaxLevelFilter(logging.Filter):
    def __init__(self, max_level: int):
        super().__init__()
        self._max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self._max_level


def setup_logging(base_dir: Path) -> Tuple[Path, Path]:
    run_date = datetime.now().strftime("%Y%m%d")
    log_dir = base_dir / "log" / run_date
    log_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%H%M%S")
    base = "run_bq_param.{}.{}".format(stamp, os.getpid())

    out_log = log_dir / (base + ".log")
    err_log = log_dir / (base + ".log.err")

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    for h in list(root.handlers):
        root.removeHandler(h)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)

    file_out = logging.FileHandler(out_log, encoding="utf-8")
    file_out.setLevel(logging.INFO)
    file_out.setFormatter(fmt)
    file_out.addFilter(_MaxLevelFilter(logging.WARNING))

    file_err = logging.FileHandler(err_log, encoding="utf-8")
    file_err.setLevel(logging.ERROR)
    file_err.setFormatter(fmt)

    root.addHandler(console)
    root.addHandler(file_out)
    root.addHandler(file_err)

    return out_log, err_log


# ============================
# CSV/JSON handling
# ============================
def read_csv_records(csv_path: Path) -> List[Dict[str, str]]:
    """Read CSV and return list of record dicts."""
    if not csv_path.exists():
        raise FileNotFoundError("CSV file not found: {}".format(csv_path))

    text = csv_path.read_text(encoding="utf-8", errors="replace")
    text = text.lstrip("\ufeff")  # Strip BOM

    reader = csv.DictReader(text.splitlines())
    records = []

    for row in reader:
        # Skip empty/comment rows
        if not row or not any(row.values()):
            continue
        first_val = next(iter(row.values()), "")
        if first_val.strip().startswith("#"):
            continue

        # Strip all values
        records.append({k: v.strip() for k, v in row.items() if k})

    return records


def save_json(json_path: Path, records: List[Dict[str, str]]) -> None:
    """Write records to JSON file."""
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


# ============================
# CLI parsing
# ============================
def parse_cli_args(argv: List[str]) -> Dict[str, str]:
    """Parse key=value arguments."""
    args = {}
    for token in argv:
        if "=" not in token:
            raise ValueError("Invalid arg (expected key=value): {}".format(token))
        key, value = token.split("=", 1)
        args[key.strip()] = value.strip()
    return args


# ============================
# BigQuery execution with parameters
# ============================
def run_bq_query_with_params(
    sql_path: Path, 
    program_id: str, 
    standard_date: str, 
    target_table: str,
    job_seq: str,
    temp_table: str
) -> None:
    """Execute BigQuery query with parameterized query using bq CLI."""
    # Read SQL file
    sql_text = sql_path.read_text(encoding="utf-8", errors="replace")

    # Build bq command with parameters
    cmd = [
        "bq", "query",
        "--quiet",
        "--use_legacy_sql=false",
        "--parameter=program_id:STRING:{}".format(program_id),
        "--parameter=standard_date:STRING:{}".format(standard_date),
        "--parameter=target_table:STRING:{}".format(target_table),
        "--parameter=job_seq:STRING:{}".format(job_seq),
        "--parameter=temp_table:STRING:{}".format(temp_table),
    ]

    # Execute query
    subprocess.run(
        cmd,
        input=sql_text,
        universal_newlines=True,
        check=True,
    )


# ============================
# Core logic
# ============================
def apply_filters(records: List[Dict[str, str]], cli_args: Dict[str, str]) -> List[Dict[str, str]]:
    """Filter records by use_yn=Y and CLI filters (mid, vs_pgm_id)."""
    # Filter by use_yn=Y (treat missing use_yn as Y)
    filtered = [r for r in records if r.get("use_yn", "Y").upper() == "Y"]

    # Apply mid filter
    if "mid" in cli_args:
        target_mid = cli_args["mid"]
        filtered = [r for r in filtered if r.get("mid", "") == target_mid]

    # Apply vs_pgm_id filter
    if "vs_pgm_id" in cli_args:
        target_pgm = cli_args["vs_pgm_id"]
        filtered = [r for r in filtered if r.get("vs_pgm_id", "") == target_pgm]

    return filtered


def execute_sql_jobs(targets: List[Dict[str, str]], overrides: Dict[str, str]) -> Tuple[int, int, int]:
    """Execute SQL for each target record. Returns (total, success, fail)."""
    total = success = fail = 0

    for record in targets:
        # Apply overrides
        effective = dict(record)
        effective.update(overrides)

        vs_pgm_id = effective.get("vs_pgm_id", "").strip()
        vs_job_dt = effective.get("vs_job_dt", "").strip()
        vs_tbl_id = effective.get("vs_tbl_id", "").strip()
        job_seq = effective.get("job_seq", "1").strip()  # Default to "1"
        temp_table = effective.get("temp_table", "").strip()

        if not vs_pgm_id:
            logger.error("Missing vs_pgm_id in record: %s", effective)
            fail += 1
            continue

        # Resolve SQL file path
        sql_path = Config.SQL_DIR / vs_pgm_id
        if not sql_path.exists():
            logger.error("SQL file not found: %s", sql_path)
            fail += 1
            continue

        # Execute
        total += 1
        logger.info(
            "%s (mid=%s, vs_job_dt=%s, vs_tbl_id=%s, job_seq=%s, temp_table=%s)",
            vs_pgm_id,
            effective.get("mid", ""),
            vs_job_dt,
            vs_tbl_id,
            job_seq,
            temp_table,
        )

        try:
            # Extract program_id from filename (without extension)
            program_id = sql_path.stem
            
            # Execute with parameters
            run_bq_query_with_params(
                sql_path=sql_path,
                program_id=program_id,
                standard_date=vs_job_dt,
                target_table=vs_tbl_id,
                job_seq=job_seq,
                temp_table=temp_table,
            )
            success += 1
        except subprocess.CalledProcessError as e:
            logger.error("bq query failed (exit_code=%s)", e.returncode)
            fail += 1

    return total, success, fail


# ============================
# Entry Point
# ============================
def main() -> int:
    out_log, err_log = setup_logging(Config.BASE_DIR)
    logger.info("SUCCESS LOG : %s", out_log)
    logger.info("ERROR LOG   : %s", err_log)

    # Parse CLI arguments
    try:
        cli_args = parse_cli_args(sys.argv[1:]) if len(sys.argv) > 1 else {}
    except ValueError as e:
        logger.error("%s", e)
        logger.error(
            "Usage: python %s [mid=<mid>] [vs_pgm_id=<file.sql>] [vs_job_dt=<yyyymmdd>] [job_seq=<seq>] [temp_table=<table>]",
            sys.argv[0],
        )
        return 1

    # Read CSV and generate JSON baseline
    try:
        records = read_csv_records(Config.CSV_PATH)
        save_json(Config.JSON_PATH, records)
        logger.info("Generated JSON baseline: %s", Config.JSON_PATH)
    except Exception as e:
        logger.error("%s", e)
        return 1

    # Filter records
    targets = apply_filters(records, cli_args)
    if not targets:
        logger.warning("No targets matched filters: %s", cli_args)
        return 0

    logger.info("Targets matched: %d", len(targets))

    # Prepare overrides (excluding filter keys)
    overrides = {k: v for k, v in cli_args.items() if k not in ("mid", "vs_pgm_id")}

    # Execute SQL jobs
    total, success, fail = execute_sql_jobs(targets, overrides)

    logger.info("=" * 50)
    logger.info("Total: %d, Success: %d, Fail: %d", total, success, fail)

    return 1 if fail > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
