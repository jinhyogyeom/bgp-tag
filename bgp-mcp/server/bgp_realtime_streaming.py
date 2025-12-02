#!/usr/bin/env python3
import pybgpstream
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
import os
import threading
import time
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL 연결 설정
POSTGRES_URI = "postgresql://bgp:bgp@bgpdb:5432/bgpdb"

class BGPRealtimeStreaming:
    def __init__(self):
        self.is_running = False
        self.batch_size = 1000
        self.batch_buffer = []
        self.buffer_lock = threading.Lock()
        self.current_date = datetime.now().strftime("%Y%m%d")
        
        # BGPStream 설정
        self.stream = pybgpstream.BGPStream(
            from_time="now-1h",  # 최근 1시간부터 시작
            until_time="now",
            collectors=["route-views2", "route-views3", "route-views4", "route-views6"],
            record_type="updates"
        )
        
        # 배치 처리 스레드
        self.batch_thread = None
        
    def start_streaming(self):
        """BGP 스트리밍 시작"""
        if self.is_running:
            logger.warning("BGP streaming is already running")
            return
            
        self.is_running = True
        
        # 배치 처리 스레드 시작
        self.batch_thread = threading.Thread(target=self._batch_processor, daemon=True)
        self.batch_thread.start()
        
        # 메인 스트리밍 루프 시작
        self._stream_loop()
        
    def stop_streaming(self):
        """BGP 스트리밍 중지"""
        self.is_running = False
        if self.batch_thread:
            self.batch_thread.join(timeout=5)
        logger.info("BGP streaming stopped")
        
    def _stream_loop(self):
        """메인 스트리밍 루프"""
        logger.info("Starting BGP realtime streaming...")
        
        try:
            for elem in self.stream:
                if not self.is_running:
                    break
                    
                # 날짜가 바뀌면 테이블명 업데이트
                new_date = datetime.now().strftime("%Y%m%d")
                if new_date != self.current_date:
                    self.current_date = new_date
                    logger.info(f"Date changed to {self.current_date}")
                
                # BGP 업데이트 메시지 처리
                if elem.type == "update":
                    self._process_bgp_update(elem)
                    
        except Exception as e:
            logger.error(f"Error in stream loop: {e}")
        finally:
            # 남은 데이터 처리
            self._flush_buffer()
            
    def _process_bgp_update(self, elem):
        """BGP 업데이트 메시지 처리"""
        try:
            # 기본 정보 추출
            timestamp = datetime.fromtimestamp(elem.time, tz=timezone.utc)
            peer_as = elem.peer_asn
            local_as = elem.collector
            
            # AS 경로 추출
            as_path = []
            if elem.fields.get('as-path'):
                as_path = [int(asn) for asn in elem.fields['as-path'].split() if asn.isdigit()]
            
            # Announce된 프리픽스들
            announce_prefixes = []
            if elem.fields.get('prefix'):
                announce_prefixes = [elem.fields['prefix']]
                
            # Withdraw된 프리픽스들
            withdraw_prefixes = []
            if elem.type == "withdraw":
                withdraw_prefixes = [elem.fields.get('prefix', '')]
            
            # 데이터 구조화 (기존 insert_to_db.py와 동일한 형식)
            update_entry = (
                timestamp,
                peer_as,
                local_as,
                announce_prefixes if announce_prefixes else None,
                withdraw_prefixes if withdraw_prefixes else None,
                as_path if as_path else None,
            )
            
            # 버퍼에 추가
            with self.buffer_lock:
                self.batch_buffer.append(update_entry)
                
        except Exception as e:
            logger.error(f"Error processing BGP update: {e}")
            
    def _batch_processor(self):
        """배치 처리 스레드"""
        while self.is_running:
            try:
                time.sleep(5)  # 5초마다 체크
                
                with self.buffer_lock:
                    if len(self.batch_buffer) >= self.batch_size:
                        batch = self.batch_buffer.copy()
                        self.batch_buffer.clear()
                    else:
                        batch = []
                
                if batch:
                    self._insert_batch(batch)
                    
            except Exception as e:
                logger.error(f"Error in batch processor: {e}")
                
    def _insert_batch(self, batch):
        """배치 데이터를 데이터베이스에 삽입"""
        try:
            table_name = f"update_entries_{self.current_date}"
            
            # 테이블 생성 (존재하지 않는 경우)
            self._create_table_if_not_exists(table_name)
            
            # 데이터베이스 연결
            conn = psycopg2.connect(POSTGRES_URI)
            cursor = conn.cursor()
            
            # 배치 삽입
            execute_values(
                cursor,
                f"""
                INSERT INTO {table_name}
                (timestamp, peer_as, local_as, announce_prefixes, withdraw_prefixes, as_path)
                VALUES %s;
                """,
                batch,
            )
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Inserted {len(batch)} BGP updates to {table_name}")
            
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            
    def _create_table_if_not_exists(self, table_name):
        """테이블이 존재하지 않으면 생성 (기존 insert_to_db.py와 동일)"""
        try:
            conn = psycopg2.connect(POSTGRES_URI)
            cursor = conn.cursor()
            
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    entry_id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ,
                    peer_as INTEGER,
                    local_as INTEGER,
                    announce_prefixes TEXT[],
                    withdraw_prefixes TEXT[],
                    as_path BIGINT[]
                );
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Table {table_name} created or verified")
            
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            
    def _flush_buffer(self):
        """남은 버퍼 데이터 처리"""
        with self.buffer_lock:
            if self.batch_buffer:
                self._insert_batch(self.batch_buffer)
                self.batch_buffer.clear()
                logger.info("Flushed remaining buffer data")


def main():
    """메인 함수"""
    streaming = BGPRealtimeStreaming()
    
    try:
        logger.info("Starting BGP realtime streaming service...")
        streaming.start_streaming()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        streaming.stop_streaming()
        logger.info("BGP streaming service stopped")


if __name__ == "__main__":
    main()
