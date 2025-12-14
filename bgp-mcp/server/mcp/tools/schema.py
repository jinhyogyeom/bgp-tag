def build_bgp_schema() -> dict:
    """Return ClickHouse bgp_updates schema and concept references."""
    return {
        "tables": {
            "bgp_updates": {
                "description": "BGP 업데이트 원시 데이터",
                "columns": {
                    "received_at": "DateTime - BGP 업데이트가 수집된 시각 (UTC)",
                    "collector": "LowCardinality(String) - 데이터를 수집한 콜렉터 식별자",
                    "peer_asn": "UInt32 - BGP 피어의 ASN(Peer AS Number)",
                    "peer_address": "String - BGP 피어의 IP 주소",
                    "prefix": "String - 프리픽스 (예: 1.0.0.0/24)",
                    "origin_as": "UInt32 - Origin AS 번호",
                    "as_path": "Array(UInt32) - AS Path 배열, 경로를 이루는 AS 번호들",
                    "next_hop": "String - Next hop IP 주소",
                    "type": "Enum8('announcement' = 1, 'withdrawal' = 2) - 업데이트 타입(공고/철회)",
                },
            }
        },
        "bgp_concepts": {
            "origin_hijack": "프리픽스의 원래 AS가 아닌 다른 AS에서 광고",
            "moas": "Multiple Origin AS - 하나의 프리픽스를 여러 AS에서 동시 광고",
            "subprefix_hijack": "더 구체적인 서브넷을 광고하여 트래픽 가로채기",
            "as_path_loop": "AS Path에서 동일한 AS가 반복되는 이상 현상",
            "prefix_flapping": "프리픽스가 짧은 시간 내에 반복적으로 광고/철회",
        },
    }
