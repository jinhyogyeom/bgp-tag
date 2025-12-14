def build_sql_examples() -> dict:
    """Return few-shot SQL examples for fetching raw bgp_updates data."""
    return {
        "guideline": "SQL 단계에서는 분석을 모두 처리하지 말고, 후속 통계/시각화 도구가 사용할 수 있도록 bgp_updates에서 필요한 최소 컬럼만 조회하세요.",
        "examples": [
            {
                "question": "특정 프리픽스(예: 1.0.0.0/24)의 최근 5분간 업데이트를 보고 싶어요",
                "sql": "SELECT received_at, prefix, origin_as, as_path, type FROM bgp_updates WHERE prefix = '1.0.0.0/24' AND received_at >= NOW() - INTERVAL '5 minute' ORDER BY received_at ASC;",
                "explanation": "특정 프리픽스와 시간 범위를 조건으로 원시 업데이트를 시간순 조회",
            },
            {
                "question": "특정 AS(AS12345)가 광고한 최근 10개의 프리픽스를 보고 싶어요",
                "sql": "SELECT received_at, prefix, origin_as, as_path FROM bgp_updates WHERE origin_as = 12345 ORDER BY received_at DESC LIMIT 10;",
                "explanation": "origin_as 필터와 LIMIT를 사용해 최신 광고 목록만 추출",
            },
            {
                "question": "collector rrc00에서 수집된 특정 시간대 데이터를 보고 싶어요",
                "sql": "SELECT received_at, collector, peer_asn, prefix, origin_as FROM bgp_updates WHERE collector = 'rrc00' AND received_at BETWEEN '2024-05-25 09:00:00' AND '2024-05-25 10:00:00' ORDER BY received_at ASC;",
                "explanation": "collector와 명시적 시간 범위를 조건으로 원시 레코드를 추출",
            },
            {
                "question": "특정 프리픽스에서 발생한 withdrawal 이벤트만 보고 싶어요",
                "sql": "SELECT received_at, prefix, origin_as, as_path, type FROM bgp_updates WHERE prefix = '45.239.179.0/24' AND type = 'withdrawal' AND received_at >= NOW() - INTERVAL '1 hour' ORDER BY received_at ASC;",
                "explanation": "type 컬럼으로 announcement/withdrawal을 구분해 필요 데이터만 조회",
            },
            {
                "question": "특정 시간대(예: 2024-05-25 09:00~09:05)에 관측된 모든 업데이트를 보고 싶어요",
                "sql": "SELECT received_at, collector, peer_asn, prefix, origin_as, as_path FROM bgp_updates WHERE received_at >= '2024-05-25 09:00:00' AND received_at < '2024-05-25 09:05:00' ORDER BY received_at ASC;",
                "explanation": "시간 범위 필터만으로 데이터를 수집한 후 후속 도구에서 세분 분석",
            },
            {
                "question": "특정 피어 ASN(예: 64512)이 관측한 업데이트만 모으고 싶어요",
                "sql": "SELECT received_at, peer_asn, peer_address, prefix, origin_as, as_path FROM bgp_updates WHERE peer_asn = 64512 AND received_at >= NOW() - INTERVAL '30 minute' ORDER BY received_at ASC;",
                "explanation": "peer_asn을 활용해 특정 피어 관점의 업데이트를 수집",
            },
        ],
        "sql_patterns": {
            "relative_time": "WHERE received_at >= NOW() - INTERVAL '5 minute'",
            "specific_time_range": "WHERE received_at BETWEEN '2024-05-25 09:00:00' AND '2024-05-25 09:05:00'",
            "prefix_filter": "WHERE prefix = '1.0.0.0/24'",
            "origin_filter": "WHERE origin_as = 12345",
            "peer_filter": "WHERE peer_asn = 64512",
            "type_filter": "WHERE type = 'announcement' OR type = 'withdrawal'",
            "ordering": "ORDER BY received_at ASC",
            "limiting": "LIMIT 10",
            "basic_columns": "SELECT received_at, prefix, origin_as, as_path, type FROM bgp_updates WHERE ...",
        },
    }
