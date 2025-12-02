#!/usr/bin/env python3
"""
BGP Stream to ClickHouse
실시간으로 BGP updates 데이터를 수집하여 ClickHouse에 저장
"""

import pybgpstream
from clickhouse_driver import Client
import time
import logging
import os
from typing import List, Optional

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BGPStreamToClickHouse:
    def __init__(self, clickhouse_host='localhost', clickhouse_port=9000, 
                 clickhouse_user='default', clickhouse_password=''):
        # 환경 변수에서 설정 읽기 (Docker 환경)
        self.clickhouse_host = os.getenv('CLICKHOUSE_HOST', clickhouse_host)
        self.clickhouse_port = int(os.getenv('CLICKHOUSE_PORT', clickhouse_port))
        self.clickhouse_user = os.getenv('CLICKHOUSE_USER', clickhouse_user)
        self.clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', clickhouse_password)
        self.client = None
        self.batch = []
        self.batch_size = 100  # 배치 단위로 insert
        
    def connect_clickhouse(self):
        """ClickHouse 연결"""
        try:
            self.client = Client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                user=self.clickhouse_user,
                password=self.clickhouse_password
            )
            logger.info(f"ClickHouse 연결 성공: {self.clickhouse_host}:{self.clickhouse_port}")
            return True
        except Exception as e:
            logger.error(f"ClickHouse 연결 실패: {e}")
            return False
    
    def parse_as_path(self, as_path_str: str) -> List[int]:
        """AS Path 문자열을 숫자 배열로 변환"""
        if not as_path_str:
            return []
        
        # AS_PATH에서 AS 번호만 추출 (AS_SET {} 등 제거)
        as_list = []
        for part in as_path_str.split():
            # AS_SET이나 다른 특수 문자 제거
            part = part.strip('{}[]')
            if part.isdigit():
                as_list.append(int(part))
        
        return as_list
    
    def process_record(self, rec):
        """BGP 레코드 처리"""
        elem = rec.get_next_elem()
        
        while elem:
            # UPDATE 타입만 처리
            if elem.type == 'A':  # Announcement (UPDATE)
                try:
                    # AS Path 파싱
                    as_path_str = elem.fields.get('as-path', '')
                    as_path = self.parse_as_path(as_path_str)
                    
                    # Origin AS는 AS Path의 마지막 AS
                    origin_as = as_path[-1] if as_path else 0
                    
                    # 데이터 구조 생성 (추가 정보 포함)
                    data = {
                        'received_at': int(rec.time),
                        'collector': rec.collector if hasattr(rec, 'collector') else '',
                        'peer_asn': int(elem.peer_asn) if hasattr(elem, 'peer_asn') and elem.peer_asn else 0,
                        'peer_address': elem.peer_address if hasattr(elem, 'peer_address') else '',
                        'prefix': elem.fields.get('prefix', ''),
                        'origin_as': origin_as,
                        'as_path': as_path,
                        'next_hop': elem.fields.get('next-hop', ''),
                        'type': 'announcement'  # elem.type == 'A'
                    }
                    
                    self.batch.append(data)
                    
                    # 배치가 가득 차면 insert
                    if len(self.batch) >= self.batch_size:
                        self.insert_batch()
                        
                except Exception as e:
                    logger.error(f"레코드 처리 중 오류: {e}")
            
            elem = rec.get_next_elem()
    
    def insert_batch(self):
        """배치 데이터를 ClickHouse에 insert"""
        if not self.batch:
            return
        
        try:
            # 배치 데이터를 튜플 리스트로 변환
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
            
            # ClickHouse에 insert
            self.client.execute(
                'INSERT INTO bgp_updates (received_at, collector, peer_asn, peer_address, prefix, origin_as, as_path, next_hop, type) VALUES',
                data_to_insert
            )
            
            logger.info(f"{len(self.batch)}개의 BGP updates 삽입 완료 (collector: {self.batch[0]['collector'] if self.batch else 'N/A'})")
            self.batch.clear()
            
        except Exception as e:
            logger.error(f"ClickHouse insert 실패: {e}")
            # 연결이 끊긴 경우 재연결 시도
            if "Connection" in str(e):
                logger.info("ClickHouse 재연결 시도...")
                self.connect_clickhouse()
    
    def start_streaming(self):
        """BGP 스트리밍 시작"""
        # ClickHouse 연결
        if not self.connect_clickhouse():
            logger.error("ClickHouse에 연결할 수 없습니다. 종료합니다.")
            return
        
        logger.info("BGP 스트리밍 시작...")
        
        # BGPStream 설정 (실시간 모드)
        # 현재 시간 (유닉스 타임스탬프)
        current_time = int(time.time())
        
        stream = pybgpstream.BGPStream(
            from_time=current_time,  # 현재 시간부터
            record_type="updates",  # updates만 수집
            project="ris-live",  # RIS Live 실시간 데이터
            collectors=["rrc00"]  # rrc00만 수집
        )
        
        logger.info("Collector: rrc00만 수집")
        
        # 실시간 데이터 수집
        try:
            for rec in stream.records():
                try:
                    self.process_record(rec)
                except Exception as e:
                    logger.error(f"레코드 처리 오류: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("사용자에 의해 중단되었습니다.")
            # 남은 배치 데이터 삽입
            if self.batch:
                self.insert_batch()
        except Exception as e:
            logger.error(f"스트리밍 중 오류 발생: {e}")
            # 남은 배치 데이터 삽입 시도
            if self.batch:
                self.insert_batch()
        finally:
            logger.info("스트리밍 종료")


def main():
    """메인 함수"""
    streamer = BGPStreamToClickHouse(
        clickhouse_host='localhost',
        clickhouse_port=9000
    )
    
    # 무한 재시도 루프
    while True:
        try:
            streamer.start_streaming()
        except Exception as e:
            logger.error(f"예외 발생: {e}")
            logger.info("10초 후 재시작...")
            time.sleep(10)


if __name__ == "__main__":
    main()

