-- TimescaleDB 확장 설치
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- hijack_events 테이블 생성 (origin, subprefix, moas 통합)
CREATE TABLE IF NOT EXISTS hijack_events (
    time TIMESTAMPTZ NOT NULL,           -- 이벤트 발생 시간 (TimescaleDB 파티셔닝 키)
    prefix TEXT NOT NULL,                 -- 영향받은 프리픽스
    event_type TEXT NOT NULL,             -- 이벤트 타입 (ORIGIN, SUBPREFIX, MOAS)
    origin_asns INTEGER[] NOT NULL,       -- 출현한 모든 origin AS 목록
    distinct_peers INTEGER NOT NULL,      -- 서로 다른 peer 수
    total_events INTEGER NOT NULL,        -- 해당 시간대의 총 이벤트 수
    first_update TIMESTAMPTZ NOT NULL,    -- 첫 번째 업데이트 시간
    last_update TIMESTAMPTZ NOT NULL,     -- 마지막 업데이트 시간
    baseline_origin INTEGER,              -- 기준 origin (ORIGIN 전용)
    top_origin INTEGER,                   -- 주도 origin (ORIGIN 전용)
    top_ratio FLOAT,                      -- 주도 origin 비율 (ORIGIN 전용)
    parent_prefix TEXT,                   -- 상위 프리픽스 (SUBPREFIX 전용)
    more_specific TEXT,                   -- 하위 프리픽스 (SUBPREFIX 전용)
    evidence_json JSONB NOT NULL,         -- 상세 증거 데이터
    summary TEXT NOT NULL,                -- 분석 요약
    analyzed_at TIMESTAMPTZ NOT NULL      -- 분석 수행 시간
);

-- loop_analysis_results 테이블 생성
CREATE TABLE IF NOT EXISTS loop_analysis_results (
    time TIMESTAMPTZ NOT NULL,           -- 이벤트 발생 시간
    prefix TEXT NOT NULL,                 -- 영향받은 프리픽스
    peer_as INTEGER NOT NULL,             -- peer AS 번호
    repeat_as INTEGER NOT NULL,           -- 반복된 AS 번호
    first_idx INTEGER NOT NULL,           -- 첫 번째 반복 위치
    second_idx INTEGER NOT NULL,          -- 두 번째 반복 위치
    as_path INTEGER[] NOT NULL,           -- AS_PATH (정수 배열)
    path_len INTEGER NOT NULL,            -- AS_PATH 길이
    summary TEXT NOT NULL,                -- 분석 요약
    analyzed_at TIMESTAMPTZ NOT NULL      -- 분석 수행 시간
);

-- moas_analysis_results 테이블은 hijack_events에 통합됨 (삭제)

-- invalid_prefix_analysis_results 테이블은 현재 사용하지 않음 (제거)

-- flap_analysis_results 테이블 생성
CREATE TABLE IF NOT EXISTS flap_analysis_results (
    time TIMESTAMPTZ NOT NULL,           -- 이벤트 발생 시간
    prefix TEXT NOT NULL,                 -- 영향받은 프리픽스
    total_events INTEGER NOT NULL,        -- 해당 시간대의 총 이벤트 수
    flap_count INTEGER NOT NULL,          -- 실제 flap 발생 횟수
    first_update TIMESTAMPTZ NOT NULL,    -- 첫 번째 업데이트 시간
    last_update TIMESTAMPTZ NOT NULL,     -- 마지막 업데이트 시간
    summary TEXT NOT NULL,                -- 분석 요약
    analyzed_at TIMESTAMPTZ NOT NULL      -- 분석 수행 시간
);

-- TimescaleDB 하이퍼테이블로 변환 (이미 하이퍼테이블이 아닌 경우에만)
SELECT create_hypertable('hijack_events', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('loop_analysis_results', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('flap_analysis_results', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- 인덱스 생성
-- 1. 시간 기반 인덱스 (TimescaleDB 자동 생성)

-- 2. 프리픽스 기반 인덱스
CREATE INDEX IF NOT EXISTS idx_hijack_prefix ON hijack_events (prefix);
CREATE INDEX IF NOT EXISTS idx_loop_prefix ON loop_analysis_results (prefix);
CREATE INDEX IF NOT EXISTS idx_flap_prefix ON flap_analysis_results (prefix);

-- 3. 시간 범위 검색을 위한 복합 인덱스
CREATE INDEX IF NOT EXISTS idx_hijack_time_range ON hijack_events (first_update, last_update);
CREATE INDEX IF NOT EXISTS idx_loop_time_range ON loop_analysis_results (time, prefix);
CREATE INDEX IF NOT EXISTS idx_flap_time_range ON flap_analysis_results (first_update, last_update);

-- 4. 특수 필드 인덱스
CREATE INDEX IF NOT EXISTS idx_hijack_origin_asns ON hijack_events USING GIN (origin_asns);
CREATE INDEX IF NOT EXISTS idx_hijack_event_type ON hijack_events (event_type);
CREATE INDEX IF NOT EXISTS idx_loop_as_path ON loop_analysis_results USING GIN (as_path);
CREATE INDEX IF NOT EXISTS idx_loop_repeat_as ON loop_analysis_results (repeat_as);
CREATE INDEX IF NOT EXISTS idx_flap_count ON flap_analysis_results (flap_count);

-- 5. 분석 시간 인덱스
CREATE INDEX IF NOT EXISTS idx_hijack_analyzed ON hijack_events (analyzed_at);
CREATE INDEX IF NOT EXISTS idx_loop_analyzed ON loop_analysis_results (analyzed_at);
CREATE INDEX IF NOT EXISTS idx_flap_analyzed ON flap_analysis_results (analyzed_at);

-- 6. 뷰 생성 (hijack 이벤트 타입별 분리)
CREATE OR REPLACE VIEW origin_hijack_events AS
SELECT * FROM hijack_events WHERE event_type = 'ORIGIN';

CREATE OR REPLACE VIEW subprefix_hijack_events AS
SELECT * FROM hijack_events WHERE event_type = 'SUBPREFIX';

CREATE OR REPLACE VIEW moas_events AS
SELECT * FROM hijack_events WHERE event_type = 'MOAS'; 