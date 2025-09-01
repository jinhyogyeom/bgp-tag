#!/usr/bin/env python3
import subprocess
from datetime import datetime
import sys
import os
from insert_to_db import check_table_exists, download_data

# ✅ 공통 경로 상수
BASE_PATH = "scripts"


# env 설정
def set_env(start_time: datetime):
    os.environ["TARGET_DATE"] = start_time.strftime("%Y%m%d")
    os.environ["BASE_PATH"] = BASE_PATH
    os.environ["TIMESCALE_URI"] = "postgresql://bgp:bgp@bgpdb:5432/bgpdb"





def run_analysis_scripts(start_time: str, end_time: str):
    scripts = [
        f"python {BASE_PATH}/scenarios/flap/flap.py",
        f"python {BASE_PATH}/scenarios/hijack/hijack.py",
        f"python {BASE_PATH}/scenarios/loop/loop.py",
        f"python {BASE_PATH}/scenarios/moas/moas.py",
    ]

    for script in scripts:
        cmd = f"{script} --start_time {start_time} --end_time {end_time}"
        result = subprocess.run(cmd, shell=True)
        if result.returncode != 0:
            sys.exit(1)


def main(start_time: datetime, end_time: datetime):
    set_env(start_time)

    if not check_table_exists(os.environ["TARGET_DATE"]):
        download_data(os.environ["TARGET_DATE"])

    run_analysis_scripts(start_time.isoformat(), end_time.isoformat())


if __name__ == "__main__":
    test_start_time = datetime(2025, 5, 25, 0, 0, 0)
    test_end_time = datetime(2025, 5, 25, 23, 59, 59)
    main(test_start_time, test_end_time)
