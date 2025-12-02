#!/usr/bin/env python3
import subprocess
from datetime import datetime
import sys
import os
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def run_single_script(name: str, script: str, start_time: str, end_time: str):
    """단일 스크립트를 실행하고 결과를 반환"""
    cmd = f"{script} --start_time {start_time} --end_time {end_time}"
    started_at = datetime.now()
    print(f"[{name}] starting: {cmd}")
    
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    
    duration = (datetime.now() - started_at).total_seconds()
    
    return {
        "name": name,
        "returncode": result.returncode,
        "duration": duration,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "started_at": started_at
    }


def run_analysis_scripts(start_time: str, end_time: str, max_workers: int = 4):
    """멀티스레딩으로 분석 스크립트들을 병렬 실행"""
    scripts = [
        ("loop", "python scenarios/loop/loop.py"),
        ("flap", "python scenarios/flap/flap.py"),
        ("moas", "python scenarios/hijack/moas.py"),
        ("origin_hijack", "python scenarios/hijack/origin_hijack.py"),
    ]

    total_started_at = datetime.now()
    print(f"[run_analysis_scripts] start: {total_started_at.isoformat()}")
    print(f"[run_analysis_scripts] running {len(scripts)} scripts in parallel with {max_workers} workers")

    results = []
    failed = False

    # ThreadPoolExecutor로 병렬 실행
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 모든 스크립트를 submit
        future_to_script = {
            executor.submit(run_single_script, name, script, start_time, end_time): name
            for name, script in scripts
        }

        # 완료되는 순서대로 결과 수집
        for future in as_completed(future_to_script):
            script_name = future_to_script[future]
            try:
                result = future.result()
                results.append(result)
                
                print(f"\n[{result['name']}] finished in {result['duration']:.2f}s with code {result['returncode']}")
                
                # stdout/stderr 출력
                if result['stdout']:
                    print(f"[{result['name']}] stdout:\n{result['stdout']}")
                if result['stderr']:
                    print(f"[{result['name']}] stderr:\n{result['stderr']}")
                
                if result['returncode'] != 0:
                    print(f"[{result['name']}] FAILED")
                    failed = True
                    
            except Exception as e:
                print(f"[{script_name}] exception occurred: {e}")
                failed = True

    # 결과 요약
    total_duration = (datetime.now() - total_started_at).total_seconds()
    print(f"\n{'='*60}")
    print(f"[run_analysis_scripts] Summary:")
    print(f"{'='*60}")
    
    # 시작 시간 순으로 정렬
    results.sort(key=lambda x: x['started_at'])
    
    for idx, result in enumerate(results, start=1):
        status = "✓ SUCCESS" if result['returncode'] == 0 else "✗ FAILED"
        print(f"[{idx}/{len(results)}] {result['name']}: {status} ({result['duration']:.2f}s)")
    
    print(f"{'='*60}")
    print(f"Total time: {total_duration:.2f}s")
    print(f"{'='*60}\n")

    if failed:
        print("[run_analysis_scripts] Some scripts failed, aborting")
        sys.exit(1)
    else:
        print("[run_analysis_scripts] All scripts completed successfully")


def main(start_time: datetime, end_time: datetime, max_workers: int = 4):
    set_env(start_time)

    # # 강제 다운로드 1회: 대상 날짜 테이블 드롭
    # drop_table_if_exists(os.environ["TARGET_DATE"])

    # if not check_table_exists(os.environ["TARGET_DATE"]):
    #     download_data(os.environ["TARGET_DATE"])

    run_analysis_scripts(start_time.isoformat(), end_time.isoformat(), max_workers=max_workers)


if __name__ == "__main__":
    test_start_time = datetime(2021, 10, 25, 0, 0, 0)
    test_end_time = datetime(2021, 10, 26, 23, 59, 59)
    
    # max_workers: 동시 실행할 최대 스레드 수 (기본값 4)
    main(test_start_time, test_end_time, max_workers=4)

