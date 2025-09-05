-- TimescaleDB 확장 설치
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- hijack_analysis_results 테이블 생성
CREATE TABLE IF NOT EXISTS hijack_analysis_results (
    time TIMESTAMPTZ NOT NULL,           -- 이벤트 발생 시간 (TimescaleDB 파티셔닝 키)
    prefix TEXT NOT NULL,                 -- 영향받은 프리픽스
    unique_origin_asns TEXT[] NOT NULL,   -- 출현한 모든 origin AS 목록
    total_events INTEGER NOT NULL,        -- 해당 시간대의 총 이벤트 수
    first_update TIMESTAMPTZ NOT NULL,    -- 첫 번째 업데이트 시간
    last_update TIMESTAMPTZ NOT NULL,     -- 마지막 업데이트 시간
    summary TEXT NOT NULL,                -- 분석 요약
    analyzed_at TIMESTAMPTZ NOT NULL      -- 분석 수행 시간
);

-- loop_analysis_results 테이블 생성
CREATE TABLE IF NOT EXISTS loop_analysis_results (
    time TIMESTAMPTZ NOT NULL,           -- 이벤트 발생 시간
    prefix TEXT NOT NULL,                 -- 영향받은 프리픽스
    as_path TEXT[] NOT NULL,              -- AS_PATH에 loop가 포함된 경로
    total_events INTEGER NOT NULL,        -- 해당 시간대의 총 이벤트 수
    first_update TIMESTAMPTZ NOT NULL,    -- 첫 번째 업데이트 시간
    last_update TIMESTAMPTZ NOT NULL,     -- 마지막 업데이트 시간
    summary TEXT NOT NULL,                -- 분석 요약
    analyzed_at TIMESTAMPTZ NOT NULL      -- 분석 수행 시간
);

-- moas_analysis_results 테이블 생성
CREATE TABLE IF NOT EXISTS moas_analysis_results (
    time TIMESTAMPTZ NOT NULL,           -- 이벤트 발생 시간
    prefix TEXT NOT NULL,                 -- 영향받은 프리픽스
    origin_asns TEXT[] NOT NULL,          -- 동시에 존재하는 origin AS 목록
    total_events INTEGER NOT NULL,        -- 해당 시간대의 총 이벤트 수
    first_update TIMESTAMPTZ NOT NULL,    -- 첫 번째 업데이트 시간
    last_update TIMESTAMPTZ NOT NULL,     -- 마지막 업데이트 시간
    summary TEXT NOT NULL,                -- 분석 요약
    analyzed_at TIMESTAMPTZ NOT NULL      -- 분석 수행 시간
);

-- invalid_prefix_analysis_results 테이블 생성
CREATE TABLE IF NOT EXISTS invalid_prefix_analysis_results (
    time TIMESTAMPTZ NOT NULL,           -- 이벤트 발생 시간
    prefix TEXT NOT NULL,                 -- 잘못된 프리픽스
    origin_asn TEXT NOT NULL,             -- 잘못된 origin AS
    total_events INTEGER NOT NULL,        -- 해당 시간대의 총 이벤트 수
    first_update TIMESTAMPTZ NOT NULL,    -- 첫 번째 업데이트 시간
    last_update TIMESTAMPTZ NOT NULL,     -- 마지막 업데이트 시간
    summary TEXT NOT NULL,                -- 분석 요약
    analyzed_at TIMESTAMPTZ NOT NULL      -- 분석 수행 시간
);

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
SELECT create_hypertable('hijack_analysis_results', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('loop_analysis_results', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('moas_analysis_results', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('invalid_prefix_analysis_results', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
SELECT create_hypertable('flap_analysis_results', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- 인덱스 생성
-- 1. 시간 기반 인덱스 (TimescaleDB 자동 생성)

-- 2. 프리픽스 기반 인덱스
CREATE INDEX IF NOT EXISTS idx_hijack_prefix ON hijack_analysis_results (prefix);
CREATE INDEX IF NOT EXISTS idx_loop_prefix ON loop_analysis_results (prefix);
CREATE INDEX IF NOT EXISTS idx_moas_prefix ON moas_analysis_results (prefix);
CREATE INDEX IF NOT EXISTS idx_invalid_prefix ON invalid_prefix_analysis_results (prefix);
CREATE INDEX IF NOT EXISTS idx_flap_prefix ON flap_analysis_results (prefix);

-- 3. 시간 범위 검색을 위한 복합 인덱스
CREATE INDEX IF NOT EXISTS idx_hijack_time_range ON hijack_analysis_results (first_update, last_update);
CREATE INDEX IF NOT EXISTS idx_loop_time_range ON loop_analysis_results (first_update, last_update);
CREATE INDEX IF NOT EXISTS idx_moas_time_range ON moas_analysis_results (first_update, last_update);
CREATE INDEX IF NOT EXISTS idx_invalid_time_range ON invalid_prefix_analysis_results (first_update, last_update);
CREATE INDEX IF NOT EXISTS idx_flap_time_range ON flap_analysis_results (first_update, last_update);

-- 4. 특수 필드 인덱스
CREATE INDEX IF NOT EXISTS idx_hijack_origin_asns ON hijack_analysis_results USING GIN (unique_origin_asns);
CREATE INDEX IF NOT EXISTS idx_loop_as_path ON loop_analysis_results USING GIN (as_path);
CREATE INDEX IF NOT EXISTS idx_moas_origin_asns ON moas_analysis_results USING GIN (origin_asns);
CREATE INDEX IF NOT EXISTS idx_invalid_origin_asn ON invalid_prefix_analysis_results (origin_asn);
CREATE INDEX IF NOT EXISTS idx_flap_count ON flap_analysis_results (flap_count);

-- 5. 분석 시간 인덱스
CREATE INDEX IF NOT EXISTS idx_hijack_analyzed ON hijack_analysis_results (analyzed_at);
CREATE INDEX IF NOT EXISTS idx_loop_analyzed ON loop_analysis_results (analyzed_at);
CREATE INDEX IF NOT EXISTS idx_moas_analyzed ON moas_analysis_results (analyzed_at);
CREATE INDEX IF NOT EXISTS idx_invalid_analyzed ON invalid_prefix_analysis_results (analyzed_at);
CREATE INDEX IF NOT EXISTS idx_flap_analyzed ON flap_analysis_results (analyzed_at); 