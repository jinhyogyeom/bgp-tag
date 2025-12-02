#!/usr/bin/env python3
"""
RIS Live WebSocket to ClickHouse
RIPE RIS Live WebSocket API를 통해 실시간 BGP 데이터를 수집하여 ClickHouse에 저장
"""

import json
import websocket
from clickhouse_driver import Client
import time
import logging
import os
from typing import List, Dict, Any

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RISLiveToClickHouse:
    def __init__(self, clickhouse_host='localhost', clickhouse_port=9000,
                 clickhouse_user='default', clickhouse_password=''):
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST', clickhouse_host)
        self.clickhouse_port = int(os.getenv('CLICKHOUSE_PORT', clickhouse_port))
        self.clickhouse_user = os.getenv('CLICKHOUSE_USER', clickhouse_user)
        self.clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', clickhouse_password)
        self.client = None
        self.batch = []
        self.batch_size = int(os.getenv('BATCH_SIZE', 1000))
        
        # WebSocket URL
        self.client_id = os.getenv('RIS_CLIENT_ID', 'python-clickhouse')
        self.ws_url = f"wss://ris-live.ripe.net/v1/ws/?client={self.client_id}"
        
        # RIS Live 구독 파라미터
        self.subscribe_params = {
            "type": "ris_subscribe",
            "data": {
                "host": os.getenv('RIS_HOST', 'rrc00'),  # rrc00, rrc01, ... 또는 전체는 생략
                "socketOptions": {
                    "includeRaw": False
                }
            }
        }
        
    def connect_clickhouse(self):
        """ClickHouse 연결"""
        try:
            self.client = Client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                user=self.clickhouse_user,
                password=self.clickhouse_password
            )
            self.ensure_table()
            logger.info(f"ClickHouse 연결 성공: {self.clickhouse_host}:{self.clickhouse_port}")
            return True
        except Exception as e:
            logger.error(f"ClickHouse 연결 실패: {e}")
            return False
    
    def ensure_table(self):
        """ClickHouse 테이블 생성"""
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS bgp_updates (
                received_at DateTime,
                collector LowCardinality(String),
                peer_asn UInt32,
                peer_address String,
                prefix String,
                origin_as UInt32,
                as_path Array(UInt32),
                next_hop String,
                type Enum8('announcement' = 1, 'withdrawal' = 2)
            )
            ENGINE = MergeTree
            ORDER BY (received_at, collector, peer_asn, prefix)
            """
            self.client.execute(create_table_sql)
            logger.info("ClickHouse 테이블 준비 완료: bgp_updates")
        except Exception as e:
            logger.error(f"테이블 생성 실패: {e}")
            raise
    
    def parse_as_path(self, as_path: List[Any]) -> List[int]:
        """AS Path를 정수 배열로 변환"""
        if not as_path:
            return []
        
        as_list = []
        for asn in as_path:
            # AS Path에 AS Set이 포함된 경우 (리스트 안에 리스트)
            if isinstance(asn, list):
                # AS Set의 첫 번째 ASN만 사용
                if asn and isinstance(asn[0], int):
                    as_list.append(asn[0])
            elif isinstance(asn, int):
                as_list.append(asn)
        
        return as_list
    
    def process_message(self, message: Dict[str, Any]):
        """RIS Live 메시지 처리"""
        try:
            msg_type = message.get('type')
            
            if msg_type == 'ris_message':
                data = message.get('data', {})
                
                # UPDATE 메시지만 처리
                bgp_type = data.get('type', '')
                if bgp_type != 'UPDATE':
                    return
                
                # 공통 필드
                timestamp = data.get('timestamp')
                collector = data.get('host', '')
                peer = data.get('peer', '')
                peer_asn_str = data.get('peer_asn', '0')
                peer_asn = int(peer_asn_str) if peer_asn_str else 0
                
                # AS Path는 메시지 최상위에 있음
                as_path = data.get('path', [])
                parsed_as_path = self.parse_as_path(as_path)
                origin_as = parsed_as_path[-1] if parsed_as_path else 0
                
                # announcements 처리
                announcements = data.get('announcements', [])
                for ann in announcements:
                    prefixes = ann.get('prefixes', [])
                    next_hop = ann.get('next_hop', '')
                    
                    for prefix in prefixes:
                        record = {
                            'received_at': int(timestamp),
                            'collector': collector,
                            'peer_asn': peer_asn,
                            'peer_address': peer,
                            'prefix': prefix,
                            'origin_as': origin_as,
                            'as_path': parsed_as_path,
                            'next_hop': next_hop,
                            'type': 'announcement'
                        }
                        self.batch.append(record)
                
                # withdrawals 처리
                withdrawals = data.get('withdrawals', [])
                for prefix in withdrawals:
                    record = {
                        'received_at': int(timestamp),
                        'collector': collector,
                        'peer_asn': peer_asn,
                        'peer_address': peer,
                        'prefix': prefix,
                        'origin_as': 0,
                        'as_path': [],
                        'next_hop': '',
                        'type': 'withdrawal'
                    }
                    self.batch.append(record)
                
                # 배치 크기 도달 시 insert
                if len(self.batch) >= self.batch_size:
                    self.insert_batch()
                    
            elif msg_type == 'ris_subscribe_ok':
                logger.info(f"✅ RIS Live 구독 성공: {message.get('data', {})}")
            elif msg_type == 'ris_error':
                logger.error(f"❌ RIS Live 에러: {message.get('data', {})}")
                
        except Exception as e:
            logger.error(f"메시지 처리 오류: {e}, message: {message}")
    
    def insert_batch(self):
        """배치 데이터를 ClickHouse에 insert"""
        if not self.batch:
            return
        
        if self.client is None:
            logger.error("ClickHouse 클라이언트가 초기화되지 않았습니다.")
            self.batch.clear()
            return
        
        try:
            data_to_insert = [
                (
                    item['received_at'],
                    item['collector'],
                    item['peer_asn'],
                    item['peer_address'],
                    item['prefix'],
                    item['origin_as'],
                    item['as_path'],
                    item['next_hop'],
                    item['type']
                )
                for item in self.batch
            ]
            
            self.client.execute(
                'INSERT INTO bgp_updates (received_at, collector, peer_asn, peer_address, prefix, origin_as, as_path, next_hop, type) VALUES',
                data_to_insert
            )
            
            logger.info(f"✅ {len(self.batch)}건 삽입 완료")
            self.batch.clear()
            
        except Exception as e:
            logger.error(f"ClickHouse insert 실패: {e}")
            self.batch.clear()
    
    def on_message(self, ws, message):
        """WebSocket 메시지 수신 핸들러"""
        try:
            parsed = json.loads(message)
            self.process_message(parsed)
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 실패: {e}")
        except Exception as e:
            logger.error(f"on_message 오류: {e}")
    
    def on_error(self, ws, error):
        """WebSocket 에러 핸들러"""
        logger.error(f"WebSocket 에러: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        """WebSocket 종료 핸들러"""
        logger.warning(f"WebSocket 연결 종료: {close_status_code} - {close_msg}")
        # 남은 배치 처리
        if self.batch:
            self.insert_batch()
    
    def on_open(self, ws):
        """WebSocket 연결 핸들러"""
        logger.info("🚀 RIS Live WebSocket 연결 성공")
        # 구독 메시지 전송
        ws.send(json.dumps(self.subscribe_params))
        logger.info(f"📡 구독 요청 전송: {self.subscribe_params['data']}")
    
    def start_streaming(self):
        """RIS Live 스트리밍 시작"""
        if not self.connect_clickhouse():
            logger.error("ClickHouse에 연결할 수 없습니다.")
            return
        
        logger.info("RIS Live 스트리밍 시작...")
        
        # WebSocket 연결 (websocket-client 라이브러리 사용)
        ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        # 영구 실행 (재연결 자동)
        ws.run_forever(
            ping_interval=30,
            ping_timeout=10
        )


def main():
    """메인 함수"""
    # 환경변수에서 설정을 읽도록 수정
    # docker-compose.yml의 환경변수를 사용
    streamer = RISLiveToClickHouse()
    
    while True:
        try:
            streamer.start_streaming()
        except KeyboardInterrupt:
            logger.info("프로그램 종료")
            break
        except Exception as e:
            logger.error(f"예외 발생: {e}")
            logger.info("10초 후 재시작...")
            time.sleep(10)


if __name__ == "__main__":
    main()

