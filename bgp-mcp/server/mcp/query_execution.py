import logging
import os
from typing import Optional, Tuple

import pandas as pd
from clickhouse_driver import Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ClickHouse 접속 기본값
DEFAULT_CLICKHOUSE_HOST = "localhost"
DEFAULT_CLICKHOUSE_PORT = 9000
DEFAULT_CLICKHOUSE_USER = "default"
DEFAULT_CLICKHOUSE_PASSWORD = ""

clickhouse_host = os.getenv("CLICKHOUSE_HOST", DEFAULT_CLICKHOUSE_HOST)
clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", DEFAULT_CLICKHOUSE_PORT))
clickhouse_user = os.getenv("CLICKHOUSE_USER", DEFAULT_CLICKHOUSE_USER)
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", DEFAULT_CLICKHOUSE_PASSWORD)


def _create_client() -> Client:
    """환경변수를 기준으로 ClickHouse 클라이언트를 생성"""
    return Client(
        host=clickhouse_host,
        port=clickhouse_port,
        user=clickhouse_user,
        password=clickhouse_password,
    )


def execute_query(sql_query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
    """SQL 쿼리를 실행하고 DataFrame 형태로 반환"""
    client = _create_client()
    logger.info(
        "ClickHouse 쿼리 실행 시작 host=%s port=%s query=%s",
        clickhouse_host,
        clickhouse_port,
        sql_query,
    )

    try:
        rows, column_types = client.execute(
            sql_query, params=params, with_column_types=True
        )
        column_names = [column[0] for column in column_types]
        df = pd.DataFrame(rows, columns=column_names)
        logger.info("쿼리 실행 성공 - 반환 행 수: %d", len(df))
        return df
    except Exception:
        logger.exception("ClickHouse 쿼리 실행 실패")
        raise
