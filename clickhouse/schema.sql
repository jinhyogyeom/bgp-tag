-- BGP Updates 테이블 스키마 (개선 버전)

-- 기존 테이블 삭제
DROP TABLE IF EXISTS bgp_updates;

-- 새 테이블 생성
CREATE TABLE bgp_updates
(
    `received_at` UInt32 COMMENT 'BGP 메시지 수신 시간 (Unix timestamp)',
    `collector` String COMMENT 'Route collector 이름 (예: rrc00, rrc01)',
    `peer_asn` UInt32 COMMENT 'BGP peer AS 번호',
    `peer_address` String COMMENT 'BGP peer IP 주소',
    `prefix` String COMMENT 'IP prefix (CIDR)',
    `origin_as` UInt32 COMMENT 'Origin AS 번호',
    `as_path` Array(UInt32) COMMENT 'AS Path (AS 번호 배열)',
    `next_hop` String COMMENT 'Next hop IP 주소',
    `type` String COMMENT 'Update 타입 (announcement/withdrawal)'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(received_at))
ORDER BY (collector, prefix, received_at)
COMMENT 'BGP Updates 실시간 수집 데이터';

-- 인덱스 추가 (선택사항)
-- ALTER TABLE bgp_updates ADD INDEX idx_peer_asn peer_asn TYPE minmax GRANULARITY 4;
-- ALTER TABLE bgp_updates ADD INDEX idx_origin_as origin_as TYPE minmax GRANULARITY 4;

