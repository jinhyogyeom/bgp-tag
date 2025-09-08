#!/usr/bin/env python3
import subprocess
from datetime import datetime
import sys
import os
import psycopg2
from insert_to_db import check_table_exists, download_data, POSTGRES_URI


# env 설정
def set_env(start_time: datetime):
    os.environ["TARGET_DATE"] = start_time.strftime("%Y%m%d")

def drop_table_if_exists(target_date: str):
    """강제 다운로드 1회를 위해 대상 날짜 테이블을 드롭"""
    table_name = f"update_entries_{target_date}"
    try:
        with psycopg2.connect(POSTGRES_URI) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {table_name};")
                conn.commit()
        print(f"Dropped table if existed: {table_name}")
    except Exception as e:
        print(f"Failed to drop table {table_name}: {e}")

def run_analysis_scripts(start_time: str, end_time: str):
    scripts = [
        ("flap",   f"python scenarios/flap/flap.py"),
        ("hijack", f"python scenarios/hijack/hijack.py"),
        ("loop",   f"python scenarios/loop/loop.py"),
        ("moas",   f"python scenarios/moas/moas.py"),
    ]

    total_started_at = datetime.now()
    print(f"[run_analysis_scripts] start: {total_started_at.isoformat()}")

    for idx, (name, script) in enumerate(scripts, start=1):
        cmd = f"{script} --start_time {start_time} --end_time {end_time}"
        started_at = datetime.now()
        print(f"[{idx}/{len(scripts)}] running {name}: {cmd}")
        result = subprocess.run(cmd, shell=True, text=True, capture_output=False)

        duration = (datetime.now() - started_at).total_seconds()
        print(f"[{idx}/{len(scripts)}] {name} finished in {duration:.2f}s with code {result.returncode}")

        # stdout/stderr는 실시간으로 출력되므로 별도 처리 불필요

        if result.returncode != 0:
            print(f"[{name}] failed, aborting")
            sys.exit(1)

    total_duration = (datetime.now() - total_started_at).total_seconds()
    print(f"[run_analysis_scripts] all done in {total_duration:.2f}s")


def main(start_time: datetime, end_time: datetime):
    set_env(start_time)

    # # 강제 다운로드 1회: 대상 날짜 테이블 드롭
    # drop_table_if_exists(os.environ["TARGET_DATE"])

    # if not check_table_exists(os.environ["TARGET_DATE"]):
    #     download_data(os.environ["TARGET_DATE"])

    run_analysis_scripts(start_time.isoformat(), end_time.isoformat())


if __name__ == "__main__":
    test_start_time = datetime(2025, 5, 25, 0, 0, 0)
    test_end_time = datetime(2025, 5, 25, 23, 59, 59)
    main(test_start_time, test_end_time)
