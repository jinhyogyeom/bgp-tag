#!/usr/bin/env python3
import subprocess
from datetime import datetime
import sys
from tqdm import tqdm
import os
from insert_to_db import check_table_exists, download_data

# ✅ 공통 경로 상수
BASE_PATH = "scripts"


# env 설정
def set_env(start_time: datetime):
    os.environ["TARGET_DATE"] = start_time.strftime("%Y%m%d")
    os.environ["BASE_PATH"] = BASE_PATH
    os.environ["TIMESCALE_URI"] = "postgresql://bgp:bgp@bgpdb:5432/bgpdb"
    os.environ["BASE_PATH"] = BASE_PATH
    os.environ["MILVUS_HOST"] = "milvus"
    os.environ["MILVUS_PORT"] = "19530"


def print_step_header(step_name: str, total_steps: int, current_step: int):
    print(f"\n{'='*50}")
    print(f"Step {current_step}/{total_steps}: {step_name}")
    print(f"{'='*50}\n")


def run_analysis_scripts(start_time: str, end_time: str):
    scripts = [
        f"python {BASE_PATH}/scenarios/flap/flap.py",
        f"python {BASE_PATH}/scenarios/hijack/hijack.py",
        f"python {BASE_PATH}/scenarios/loop/loop.py",
        f"python {BASE_PATH}/scenarios/moas/moas.py",
    ]

    print("\nRunning Analysis Scripts:")
    for script in tqdm(scripts, desc="Analysis Progress", unit="script"):
        print(f"\nExecuting: {script}")
        cmd = f"{script} --start_time {start_time} --end_time {end_time}"
        result = subprocess.run(cmd, shell=True)
        if result.returncode != 0:
            print(f"❌ Error running {script}")
            sys.exit(1)
        print(f"✅ Completed: {script}")


def run_report_scripts(start_time: str, end_time: str):
    scripts = [
        f"python {BASE_PATH}/scenarios/flap/flap_report.py --output_file {BASE_PATH}/flap_10min_nl_reports.jsonl",
        f"python {BASE_PATH}/scenarios/hijack/hijack_report.py --output_file {BASE_PATH}/hijack_10min_nl_reports.jsonl",
        f"python {BASE_PATH}/scenarios/loop/loop_report.py --output_file {BASE_PATH}/loop_10min_nl_reports.jsonl",
        f"python {BASE_PATH}/scenarios/moas/moas_report.py --output_file {BASE_PATH}/moas_10min_nl_reports.jsonl",
    ]

    print("\nGenerating Reports:")
    for script in tqdm(scripts, desc="Report Generation", unit="report"):
        print(f"\nGenerating: {script}")
        cmd = f"{script} --start_time {start_time} --end_time {end_time}"
        result = subprocess.run(cmd, shell=True)
        if result.returncode != 0:
            print(f"❌ Error generating {script}")
            sys.exit(1)
        print(f"✅ Completed: {script}")


def run_milvus_embedding():
    print("\nRunning Milvus Embedding:")
    cmd = f"python {BASE_PATH}/vector_db/embed_to_milvus.py"
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print("❌ Error running Milvus embedding")
        sys.exit(1)
    print("✅ Milvus embedding completed")


def print_pipeline_status(
    start_time: datetime, end_time: datetime, current_step: int, total_steps: int
):
    print(f"\n{'='*50}")
    print(f"Pipeline Status:")
    print(f"Time Range: {start_time} to {end_time}")
    print(f"Progress: {current_step}/{total_steps} steps completed")
    print(f"Completion: {(current_step/total_steps)*100:.1f}%")
    print(f"{'='*50}\n")


def main(start_time: datetime, end_time: datetime):
    total_steps = 3

    set_env(start_time)

    if not check_table_exists(os.environ["TARGET_DATE"]):
        download_data(os.environ["TARGET_DATE"])

    print_step_header("Data Analysis", total_steps, 1)
    run_analysis_scripts(start_time.isoformat(), end_time.isoformat())
    print_pipeline_status(start_time, end_time, 1, total_steps)

    print_step_header("Report Generation", total_steps, 2)
    run_report_scripts(start_time.isoformat(), end_time.isoformat())
    print_pipeline_status(start_time, end_time, 2, total_steps)

    print_step_header("Milvus Embedding", total_steps, 3)
    run_milvus_embedding()
    print_pipeline_status(start_time, end_time, 3, total_steps)

    print(f"\n✅ Pipeline completed successfully.")


# 예시용 직접 실행
if __name__ == "__main__":
    # 테스트용 고정 시간 예시
    test_start_time = datetime(2025, 5, 25, 0, 0, 0)
    test_end_time = datetime(2025, 5, 25, 23, 59, 59)
    main(test_start_time, test_end_time)
