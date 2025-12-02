from datetime import datetime
import os
import psycopg2
from psycopg2.extras import execute_values
import mrtparse

POSTGRES_URI = "postgresql://postgres:postgres@timescaledb:5432/bgp_timeseries"


def check_table_exists(target_date):
    conn = psycopg2.connect(POSTGRES_URI)
    table_name = f"update_entries_{target_date}"

    cursor = conn.cursor()
    cursor.execute(
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');"
    )
    table_exists = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return table_exists


def download_data(target_date):
    def download_and_process_file(hour: int, minute: str):
        hour_str = f"{hour:02d}"
        print(
            f"Downloading and processing file for {target_date} {hour_str}:{minute}..."
        )
        file_base = f"./routeviews_data/updates.{target_date}.{hour_str}{minute}"
        bz2_file = f"{file_base}.bz2"
        url = f"https://routeviews.org/bgpdata/{target_date[:4]}.{target_date[4:6]}/UPDATES/updates.{target_date}.{hour_str}{minute}.bz2"
        print(url)
        try:
            os.system(f"curl -o {bz2_file} {url}")
            os.system(f"bzip2 -d {bz2_file}")

            insert_update_entries(
                db_uri=POSTGRES_URI,
                file_path=file_base,
                table_name=f"update_entries_{target_date}",
            )

            if os.path.exists(file_base):
                os.remove(file_base)

        except KeyboardInterrupt:
            print(
                f"\n키보드 인터럽트 발생. 현재 처리 중인 파일: {target_date}.{hour_str}{minute}"
            )
            for file in [bz2_file, file_base]:
                if os.path.exists(file):
                    os.remove(file)
            raise

    try:
        for hour in range(0, 24):
            for minute in ["00", "15", "30", "45"]:
                download_and_process_file(hour, minute)

    except KeyboardInterrupt:
        print("\n프로그램이 사용자에 의해 중단되었습니다.")
        raise


def create_table_if_not_exists(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                entry_id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ,
                peer_as INTEGER,
                local_as INTEGER,
                announce_prefixes TEXT[],
                withdraw_prefixes TEXT[],
                as_path BIGINT[]
            );
        """
        )
        conn.commit()


def insert_update_entries(db_uri, file_path, table_name):
    print(f"Parsing and inserting UPDATE MRT file: {file_path} into PostgreSQL...")

    conn = psycopg2.connect(db_uri)

    create_table_if_not_exists(conn, table_name)

    parser = mrtparse.Reader(file_path)
    batch_size = 1000
    batch = []

    for idx, entry in enumerate(parser):
        try:
            if "bgp_message" not in entry.data:
                continue

            bgp_message = entry.data["bgp_message"]

            timestamp = None
            if "timestamp" in entry.data:
                timestamp = list(entry.data["timestamp"].values())[0]

            announce_prefixes = [
                nlri["prefix"] + "/" + str(nlri["length"])
                for nlri in bgp_message.get("nlri", [])
            ]
            withdraw_prefixes = [
                withdrawn["prefix"]
                for withdrawn in bgp_message.get("withdrawn_routes", [])
            ]

            as_path = []
            for attr in bgp_message.get("path_attributes", []):
                if next(iter(attr.get("type")), None) == 2:
                    for as_seq in attr.get("value", []):
                        if isinstance(as_seq, dict) and "value" in as_seq:
                            as_path.extend(map(int, as_seq["value"]))

            update_entry = (
                timestamp,
                entry.data.get("peer_as"),
                entry.data.get("local_as"),
                announce_prefixes if announce_prefixes else None,
                withdraw_prefixes if withdraw_prefixes else None,
                as_path if as_path else None,
            )

            batch.append(update_entry)

            if len(batch) >= batch_size:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO {table_name}
                        (timestamp, peer_as, local_as, announce_prefixes, withdraw_prefixes, as_path)
                        VALUES %s;
                    """,
                        batch,
                    )
                    conn.commit()
                batch.clear()

        except Exception as e:
            print(f"Error processing UPDATE entry {idx + 1}: {e}")
            continue

    # 남은 데이터 삽입
    if batch:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {table_name}
                (timestamp, peer_as, local_as, announce_prefixes, withdraw_prefixes, as_path)
                VALUES %s;
            """,
                batch,
            )
            conn.commit()

    conn.close()
    print(f"✅ PostgreSQL에 데이터 저장 완료: {table_name}")
