쇼#!/usr/bin/env python3
import logging
import csv
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import NamedTuple, Optional, Dict, List, Tuple

logger = logging.getLogger(__name__)

# ============================
# Config
# ============================
class Config:
    BASE_DIR = Path(__file__).resolve().parents[1]
    SQL_DIR = BASE_DIR / "sql_param"
    SRC_DIR = BASE_DIR/"sql"
    ENV_DIR = SRC_DIR/"env"
    MID_ENV_JSON = ENV_DIR/"mid_env.json"
    MID_JSON_FALLBACK = ENV_DIR/"mid.json"

    DW_LIST = ENV_DIR/"dw/bq.list"
    DM_LIST = ENV_DIR/"dm/bq.list"
    DW_JSON = ENV_DIR/"dw/bq.json"
    DM_JSON = ENV_DIR/"dm/bq.json"
    
    DRY_RUN = os.environ.get("DRY_RUN","false").lower() == "true"

class LazyErrorFileHandler(logging.Handler):
	def __init__(self,err_path,formatter):
		logging.Handler.__init__(self,level=logging.ERROR)
		self._err_path = PAth(err_path)
		self._formatter = formatter
		self._fh = None

	def emit(self, record):
		try:
			if self._fh is None:
				self._err_path.parent.mkdir(parents=True, exist_ok=True)
				self._fh = logging.FileHandler(str(self._err_path),encoding="utf-8")
				self._fh.setLevel(logging.ERROR)
				self._fh.setFormatter(self._formatter)
			self._fh.emit(record)
		except Exception:
			self.handlerError(record)

	def close(self):
		try:
			if self._fh is not None:
				self._fh.close()
		finally:
			logging.Handler.clse(self)

def setup_logging(base_dir: Path) -> Tuple[Path, Path]:
    run_date = datetime.now().strftime("%Y%m%d")
    log_dir = base_dir / "log" / run_date
    log_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%H%M%S")
    base = "run_bq_param.{}.{}".format(stamp, os.getpid())

    out_log = log_dir / (base + ".log")
    err_log = log_dir / (base + ".log.err")

    #fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fmt = logging.Formatter("{asctime} [{levelname}] {message}", style="{")
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
    
		 lazy_err = LazyErrorFileHandler(err_log,fmt)

    root.addHandler(console)
    root.addHandler(file_out)
    root.addHandler(lazy_err)

    return out_log, err_log


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
# CSV/JSON handling
# ============================
def read_list_csv(list_path: Path) -> List[Dict[str, str]]:
    list_path = Path(list_path)
    if not csv_path.exists():
        raise FileNotFoundError("CSV file not found: {}".format(csv_path))

    text = list_path.read_text(encoding="utf-8", errors="replace")
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

def load_min_env():
	path = Config.MID_ENV_JSON if Config.MID_ENV_JSON.exists() else Config.MID_JSON_FALLBACK
	if not path.exists():
		raise FileNotFoundError(f"Missing mid env json: {path}")
	data = json.loads(path.read_text(encoding="utf-8",errors="replace"))
	if not isinstance(data, dict):
		raise ValueError("Invalid mid env json formatt")
	return path, data

def normalize_use_yn(v):
	return (v or "Y").strip().upper() == "Y"

def record_matches_pgm_id(record,pgm_id):
	if not pgm_id:
		return True

	target = pgm_id.strip()
	rec_mid = (record.get("mid") or "").strip()
  ec_pgm = (record.get("pgm_id") or "").strip()

	candidates = [rec_pgm]
	if rec_mid and rec_pgm:
		candidates.append(rec_mid + "/" + rec_pgm)

	return target in candidates

def split_mid_and_name(pgm_id):
	s = (pgm_id or "").strip()
	if "/" in s:
		left, right = s.aplit("/",1)
		return left.strip(), right.strip()
	return "",s

def resolve_sql_path(program_type, record_mid, pgm_id):
	if record_mid == "bm":
		sql_root = Config.SQL_DIR
	else:
		sql_root = Config.SQL_DIR / program_type
	mid_from_id, name = split_mid_and_name(pgm_id)
	return sql_root / name

def build_standard_date(cli_args, mid_env_section, record):
	if "job_d" in cli_args and cli_args["job_d"]:
		return cli_args["job_d"].strip()

	env_date = (mid_env_section.get("job_d") or "").strip()
	if env_date:
		return env_date
	return (record.get("job_d") or "").strip()

def build_params(program_type, mid_env_seciotn, record, cli_args):
	params = {}
	pgm_id = (record.get("pgm_id") or "").strip()
	_, filename = split_mid_and_name(pgm_id)
	program_id = Path(filename).stem

	standard_date = build_stadard_date(cli_args, mid_env_section, record)

	if program_type == "dw":
		target_table = (
			cli_args.get("target_table") or record.get("target_table") or "").strip() 
			job_seq.get("job_seq") or record.get("job_seq") or "").strip()
			temp_table.get("temp_table") or record.get("temp_table") or "").strip()

		params.update(
			{
				"program_id": program_id,
				"statnard_date": standard_date,
				"target_table": target_table,
				"job_seq": job_seq,
				"temp_table": temp_table,
			}
		)
	elif program_type == "dm":
		table_name = (cli_args.get("tbl_nm") or record.get("tbl_nm") or "").strip()
		params.update(
			{
				"program_id": program_id,
				"standard_date": standard_date,
				"table_name": table_name,
			}
		)
	else:
		raise ValueError(f"Unknown program_type: {program_type}")

	declared = mid_env_section.get("params") or []
	declared = [str(x) for x in declared]

	return {k: params.get(k,"") fro k in declared}

def run_bq_query(sql_path, param_dict):
	sql_text = Path(sql_path).read_text(encoding="utf-8",errors="replace")
	cmd = ["bq","query", "--quiet", "--use_legacy_sql=false",]

	for k,v in param_dict.items():
		cmd.append(f"--parameter={k}:STRING:{v}")

	if Config.DRY_RUN:
		logger.info("-"*60)
		return

	subprocess.run(
		cmd,
		input=sql_text,
		universal_newlines=True,
		stdout=subprocess.PIPE,
		stderr=subprocess.PIPE,
		check=Treu,
	)

def generate_baseline()
	dw_records = read_list_csv(Confing.DW_LIST)
	dm_records = read_list_csv(Confing.DM_LIST)
	save_json(Config.DW_JSON, dw_records)
	save_json(Config.DM_JSON, dm_records)

	return dw_records, dm_records

def filter_targets(records, cli_args):
	targets = [r for r in records if normalize_use_yn(r.get("use_yn"))]

 if "mid" in cli_args and cli_args["mid"] and cli_args["mid"] != "all":
		mid = cli_args["mid"].strip()
		targets = [r for r in targets if (r.get("mid") or "").strip() == mid]
	if "pgm_id" in cli_args and cli_args["pgm_id"]:
		pgm_id = cli_args["pgm_id"].strip()
		targets = [r for r in targets if record_matches_pgm_id(r,pgm_id)]

	return targets

def execute_jobs(program_type, mid_env_section, targets, cli_args):
	total = success = fail = done = 0

	all_cnt = sum(1 for r in targets)

	for record in targets:
		record_mid = (record.get("mid") or "").strip()
		pgm_id = (record.get("pgm_id") or "").strip()

		total += 1

		sql_path = resolve_sql_path(program_type,record_mid,pgm_id)
		if not sql_path.exists():
			fail += 1
			done += 1
			continue

		params_dict = build_params(program_type, mid_env_section,record,cli_args)

		for name in params_dict:
			logger.info("+----")
		try:
			run_bq_query(sql_path, params_dict)
			success += 1
		except subprocess.CalledProcessError as e:
			if e.stdout:
				logger.error(f" stdout: {e.stdout}")
			if e.stderr:
				logger.error(f" stderr: {e.stderr}")
			fail += 1
		except Exception as e:
			fail += 1
		finally:
			done += 1
	return total, done, success, fail

def main() -> int:
	try:
		cli_args = parse_cli_args(sys.argv[1:]) if len(sys.argv) > 1 else {}
	except ValueError as e:
		logger.error("%s",e)
		return 1

	mid_for_log = (cli_args.get("mid") or "nomid").strip() or "nomid"
	out_log, err_log = setup_logging(Config.BASE_DIR,mid_for_log)

	try:
		env_path,mid_env = load_min_env()
	except Exception as e:
		logger.error(f"{e}")
		return 1

	try:
		dw_records, dm_records = generate_baseline()
	except Exception as e:
		return 1

	want_mid = (cli_args.get("mid") or "").strip()
	run_dw_all = want_mid == "dw_all"
	run_dm_all = want_mid == "dm_all"

	totals = {"total":0, "done":0, "success":0, "fail":0}

	dw_section = mid_env.get("dw") or {}
	dw_cli_args = dict(cli_args)
	if run_dw_all:
		dw_cli_args.pop("mid",None)
	dw_targets = filter_targets(dw_records, dw_cli_args)
	if run_dw_all or not want_mid or (want_mid and want_mid in [m for m in (dw_section.get("mids") or [])]):
		if not run_dm_all and dw_targets:
			t,d,s,f = execute_jobs("dw",dw_section,dw_targets,dw_cli_args)
			totals["total"] += t  
			totals["done"] += d 
			totals["success"] += s
			totals["fail"] += f

	dm_section = mid_env.get("dm") or {}
	dm_cli_args = dict(cli_args)
	if run_dm_all:
		dm_cli_args.pop("mid",None)
	dm_targets = filter_targets(dm_records, dm_cli_args)

	if run_dm_all or not want_mid or (want_mid and want_mid in [m for m in (dm_section.get("mids") or [])]):
		if not run_dw_all and dm_targets:
			t,d,s,f = execute_jobs("dm",dm_section,dm_targets,dm_cli_args)
			totals["total"] += t  
			totals["done"] += d 
			totals["success"] += s
			totals["fail"] += f
	if totals["total"] == 0:
		return 0

	return 1 if totals["fail"] > 0 else 0

if __name__ == "__main__":
	raise SystemExit(main()) 




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
