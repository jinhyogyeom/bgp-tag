import json
import pandas as pd
from fastmcp import FastMCP
from query_execution import execute_query

# FastMCP 서버 초기화
mcp = FastMCP(
    name="BGP Analysis Server",
    instructions="BGP 네트워크 데이터 분석 도구 제공 - 클라이언트가 전문가 역할 수행"
)

@mcp.tool()
def get_system_instructions() -> str:
    """BGP 분석 전문가 시스템 지침을 제공합니다."""
    instructions = {
        "role": "BGP(Border Gateway Protocol) 네트워크 분석 전문가",
        "responsibilities": [
            "BGP 이상 탐지 및 네트워크 보안 분석 전문가",
            "사용자의 질문을 분석하여 적절한 SQL 쿼리 작성",
            "쿼리 결과를 전문적으로 해석하고 인사이트 제공",
            "BGP 관련 용어와 개념을 쉽게 설명"
        ],
        "analysis_process": [
            "1. 먼저 get_bgp_schema()로 테이블 구조와 컬럼 정보 파악",
            "2. get_sql_examples()로 유사한 쿼리 패턴과 예제 참조",
            "3. 사용자 질문에 맞는 정확한 SQL 쿼리 작성",
            "4. execute_bgp_query()로 데이터 조회",
            "5. 결과를 전문적으로 분석하고 설명"
        ],
        "database_info": "PostgreSQL TimescaleDB (시계열 데이터 최적화)",
        "bgp_concepts": {
            "Origin Hijack": "프리픽스의 원래 AS가 아닌 다른 AS에서 광고",
            "MOAS": "Multiple Origin AS - 하나의 프리픽스를 여러 AS에서 동시 광고",
            "AS Path Loop": "AS Path에서 동일한 AS가 반복되는 이상 현상",
            "Prefix Flapping": "프리픽스가 짧은 시간 내에 반복적으로 광고/철회"
        },
        "guidelines": [
            "항상 스키마와 예제를 참조하여 정확하고 전문적인 분석을 제공하세요.",
            "시간대, prefix, as 등이 일치하는 데이터가 존재하지 않는 경우 없는 결과를 지어내지 말고 관측된 데이터가 없다고 명시하세요."
        ]ㄹ
    }
    
    return json.dumps(instructions, ensure_ascii=False, indent=2)

@mcp.tool()
def get_bgp_schema() -> str:
    """BGP 데이터베이스 테이블 스키마 정보 제공"""
    schema = {
        "tables": {
            "bgp_updates": {
                "description": "BGP 업데이트 원시 데이터",
                "columns": {
                    "time": "TIMESTAMPTZ - BGP 업데이트 시간",
                    "prefix": "TEXT - 프리픽스 (예: 1.0.0.0/24)",
                    "peer_as": "INTEGER - Peer AS 번호",
                    "origin_as": "INTEGER - Origin AS 번호",
                    "as_path": "INTEGER[] - AS Path 배열",
                    "next_hop": "TEXT - Next hop IP",
                    "update_type": "TEXT - announce/withdraw"
                }
            },
            "hijack_events": {
                "description": "하이재킹 이벤트 통합 테이블",
                "columns": {
                    "time": "TIMESTAMPTZ - 이벤트 발생 시간",
                    "prefix": "TEXT - 영향받은 프리픽스",
                    "event_type": "TEXT - ORIGIN/SUBPREFIX/MOAS",
                    "origin_asns": "INTEGER[] - 출현한 모든 origin AS 목록",
                    "distinct_peers": "INTEGER - 서로 다른 peer 수",
                    "total_events": "INTEGER - 총 이벤트 수",
                    "first_update": "TIMESTAMPTZ - 첫 번째 업데이트 시간",
                    "last_update": "TIMESTAMPTZ - 마지막 업데이트 시간",
                    "baseline_origin": "INTEGER - 기준 origin AS",
                    "top_origin": "INTEGER - 주도 origin AS",
                    "top_ratio": "FLOAT - 주도 origin 비율",
                    "parent_prefix": "TEXT - 상위 프리픽스 (SUBPREFIX 전용)",
                    "more_specific": "TEXT - 하위 프리픽스 (SUBPREFIX 전용)",
                    "evidence_json": "JSONB - 상세 증거 데이터",
                    "summary": "TEXT - 이벤트 요약",
                    "analyzed_at": "TIMESTAMPTZ - 분석 수행 시간"
                }
            },
            "loop_analysis_results": {
                "description": "AS Path 루프 분석 결과",
                "columns": {
                    "time": "TIMESTAMPTZ - 이벤트 발생 시간",
                    "prefix": "TEXT - 영향받은 프리픽스",
                    "peer_as": "INTEGER - Peer AS 번호",
                    "repeat_as": "INTEGER - 반복된 AS 번호",
                    "first_idx": "INTEGER - 첫 번째 반복 위치",
                    "second_idx": "INTEGER - 두 번째 반복 위치",
                    "as_path": "INTEGER[] - AS Path 배열",
                    "path_len": "INTEGER - AS Path 길이",
                    "summary": "TEXT - 분석 요약",
                    "analyzed_at": "TIMESTAMPTZ - 분석 수행 시간"
                }
            },
            "flap_analysis_results": {
                "description": "프리픽스 플래핑 분석 결과",
                "columns": {
                    "time": "TIMESTAMPTZ - 이벤트 발생 시간",
                    "prefix": "TEXT - 플래핑된 프리픽스",
                    "total_events": "INTEGER - 총 이벤트 수",
                    "flap_count": "INTEGER - 실제 flap 발생 횟수",
                    "first_update": "TIMESTAMPTZ - 첫 번째 업데이트 시간",
                    "last_update": "TIMESTAMPTZ - 마지막 업데이트 시간",
                    "summary": "TEXT - 분석 요약",
                    "analyzed_at": "TIMESTAMPTZ - 분석 수행 시간"
                }
            }
        },
        "bgp_concepts": {
            "origin_hijack": "프리픽스의 원래 AS가 아닌 다른 AS에서 광고",
            "moas": "Multiple Origin AS - 하나의 프리픽스를 여러 AS에서 동시 광고",
            "subprefix_hijack": "더 구체적인 서브넷을 광고하여 트래픽 가로채기",
            "as_path_loop": "AS Path에서 동일한 AS가 반복되는 이상 현상",
            "prefix_flapping": "프리픽스가 짧은 시간 내에 반복적으로 광고/철회"
        }
    }
    
    return json.dumps(schema, ensure_ascii=False, indent=2)

@mcp.tool()
def get_sql_examples() -> str:
    """BGP 분석을 위한 Few-shot 예제들을 제공합니다."""
    examples = {
        "examples": [
            {
                "question": "최근 24시간 동안 발생한 하이재킹 이벤트를 알려주세요",
                "sql": "SELECT * FROM hijack_events WHERE time >= NOW() - INTERVAL '24 hours' ORDER BY time DESC LIMIT 10;",
                "explanation": "최근 24시간의 하이재킹 이벤트를 시간 역순으로 조회"
            },
            {
                "question": "특정 AS(예: AS12345)와 관련된 모든 이상현상을 알려주세요",
                "sql": "SELECT 'hijack' as event_type, time, prefix, baseline_origin as origin_as, top_origin as target_as, NULL::integer[] as as_path, summary FROM hijack_events WHERE baseline_origin = 12345 OR top_origin = 12345 UNION ALL SELECT 'loop' as event_type, time, prefix, peer_as as origin_as, repeat_as as target_as, as_path, summary FROM loop_analysis_results WHERE peer_as = 12345 OR repeat_as = 12345 UNION ALL SELECT 'flap' as event_type, time, prefix, total_events as origin_as, flap_count as target_as, NULL::integer[] as as_path, summary FROM flap_analysis_results ORDER BY time DESC;",
                "explanation": "AS12345와 관련된 모든 이상현상을 통일된 컬럼 구조로 통합 조회"
            },
            {
                "question": "Origin Hijack 이벤트에 대해 알려주세요",
                "sql": "SELECT * FROM hijack_events WHERE event_type = 'origin_hijack' ORDER BY time DESC LIMIT 20;",
                "explanation": "Origin Hijack 타입의 이벤트만 조회"
            },
            {
                "question": "가장 많은 플래핑이 발생한 프리픽스들을 알려주세요",
                "sql": "SELECT prefix, MAX(flap_count) as max_flaps FROM flap_analysis_results GROUP BY prefix ORDER BY max_flaps DESC LIMIT 5;",
                "explanation": "프리픽스별 최대 플래핑 횟수를 집계하여 상위 5개 조회"
            },
            {
                "question": "AS Path 루프 이벤트가 얼마나 발생했는지 알려주세요",
                "sql": "SELECT COUNT(*) as loop_count FROM loop_analysis_results;",
                "explanation": "AS Path 루프 이벤트의 총 개수 조회"
            },
            {
                "question": "특정 프리픽스(예: 1.0.0.0/24)와 관련된 모든 이벤트를 알려주세요",
                "sql": "SELECT * FROM hijack_events WHERE prefix = '1.0.0.0/24' ORDER BY time DESC;",
                "explanation": "특정 프리픽스와 관련된 모든 하이재킹 이벤트 조회"
            },
            {
                "question": "특정 프리픽스(예: 45.239.179.0/24)에서 특정 날짜(2025-05-25)에 발생한 모든 이상현상을 분석해주세요",
                "sql": "SELECT 'hijack' as event_type, time, prefix, baseline_origin as origin_as, top_origin as target_as, NULL::integer[] as as_path, summary FROM hijack_events WHERE prefix = '45.239.179.0/24' AND time::date = '2025-05-25' UNION ALL SELECT 'loop' as event_type, time, prefix, peer_as as origin_as, repeat_as as target_as, as_path, summary FROM loop_analysis_results WHERE prefix = '45.239.179.0/24' AND time::date = '2025-05-25' UNION ALL SELECT 'flap' as event_type, time, prefix, total_events as origin_as, flap_count as target_as, NULL::integer[] as as_path, summary FROM flap_analysis_results WHERE prefix = '45.239.179.0/24' AND time::date = '2025-05-25' ORDER BY time;",
                "explanation": "특정 프리픽스와 날짜의 모든 이상현상을 통일된 구조로 시간순 조회"
            },
            {
                "question": "2024년 1월 15일 오전 9시부터 오후 6시까지 발생한 모든 이상현상을 알려주세요",
                "sql": "SELECT 'hijack' as event_type, time, prefix, baseline_origin as origin_as, top_origin as target_as, NULL::integer[] as as_path, summary FROM hijack_events WHERE time >= '2024-01-15 09:00:00' AND time <= '2024-01-15 18:00:00' UNION ALL SELECT 'loop' as event_type, time, prefix, peer_as as origin_as, repeat_as as target_as, as_path, summary FROM loop_analysis_results WHERE time >= '2024-01-15 09:00:00' AND time <= '2024-01-15 18:00:00' UNION ALL SELECT 'flap' as event_type, time, prefix, total_events as origin_as, flap_count as target_as, NULL::integer[] as as_path, summary FROM flap_analysis_results WHERE time >= '2024-01-15 09:00:00' AND time <= '2024-01-15 18:00:00' ORDER BY time;",
                "explanation": "특정 시간 범위(2024-01-15 09:00~18:00)의 모든 이상현상을 통일된 컬럼 구조로 통합 조회"
            },
            {
                "question": "2024년 2월 1일 하루 동안 발생한 Origin Hijack 이벤트를 알려주세요",
                "sql": "SELECT * FROM hijack_events WHERE event_type = 'origin_hijack' AND time >= '2024-02-01 00:00:00' AND time < '2024-02-02 00:00:00' ORDER BY time;",
                "explanation": "특정 날짜(2024-02-01)의 Origin Hijack 이벤트를 시간순으로 조회"
            },
            {
                "question": "2024년 3월 15일에 가장 많은 이상현상이 발생한 프리픽스들을 알려주세요",
                "sql": "SELECT prefix, event_type, COUNT(*) as count FROM (SELECT prefix, 'hijack' as event_type FROM hijack_events WHERE time >= '2024-03-15 00:00:00' AND time < '2024-03-16 00:00:00' UNION ALL SELECT prefix, 'loop' as event_type FROM loop_analysis_results WHERE time >= '2024-03-15 00:00:00' AND time < '2024-03-16 00:00:00' UNION ALL SELECT prefix, 'flap' as event_type FROM flap_analysis_results WHERE time >= '2024-03-15 00:00:00' AND time < '2024-03-16 00:00:00') all_anomalies GROUP BY prefix, event_type ORDER BY count DESC;",
                "explanation": "특정 날짜의 모든 이상현상을 종류별로 구분하여 프리픽스별 집계"
            },
            {
                "question": "최근 1주일 동안 어떤 이상현상들이 발생했나요?",
                "sql": "SELECT event_type, COUNT(*) as total_count, COUNT(DISTINCT prefix) as affected_prefixes FROM (SELECT 'hijack' as event_type, prefix FROM hijack_events WHERE time >= NOW() - INTERVAL '7 days' UNION ALL SELECT 'loop' as event_type, prefix FROM loop_analysis_results WHERE time >= NOW() - INTERVAL '7 days' UNION ALL SELECT 'flap' as event_type, prefix FROM flap_analysis_results WHERE time >= NOW() - INTERVAL '7 days') all_anomalies GROUP BY event_type ORDER BY total_count DESC;",
                "explanation": "최근 1주일간 모든 이상현상 종류별 통계 (총 발생 횟수와 영향받은 프리픽스 수)"
            }
        ],
        "sql_patterns": {
            "relative_time": "WHERE time >= NOW() - INTERVAL '24 hours'",
            "specific_time_range": "WHERE time >= '2024-01-15 09:00:00' AND time <= '2024-01-15 18:00:00'",
            "specific_date": "WHERE time >= '2024-02-01 00:00:00' AND time < '2024-02-02 00:00:00'",
            "date_filter": "WHERE time::date = '2025-05-25'",
            "ordering": "ORDER BY time DESC",
            "limiting": "LIMIT 10",
            "counting": "SELECT COUNT(*) as count FROM table_name",
            "grouping": "GROUP BY column_name ORDER BY count DESC",
            "event_type_filter": "WHERE event_type = 'origin_hijack'",
            "as_filtering": "WHERE baseline_origin = AS_NUMBER OR hijacker_origin = AS_NUMBER",
            "union_all_unified": "SELECT 'hijack' as event_type, time, prefix, baseline_origin as origin_as, hijacker_origin as target_as, NULL::integer[] as as_path, summary FROM hijack_events WHERE ... UNION ALL SELECT 'loop' as event_type, time, prefix, peer_as as origin_as, repeat_as as target_as, as_path, summary FROM loop_analysis_results WHERE ... UNION ALL SELECT 'flap' as event_type, time, prefix, peer_as as origin_as, flap_count as target_as, NULL::integer[] as as_path, summary FROM flap_analysis_results WHERE ...",
            "avoid_select_star": "절대 SELECT * 와 UNION ALL을 함께 사용하지 말것 - 컬럼 수 불일치 오류 발생"
        }
    }
    
    return json.dumps(examples, ensure_ascii=False, indent=2)

@mcp.tool()
def execute_bgp_query(sql_query: str, params: str = None) -> str:
    """SQL 쿼리를 실행하고 결과를 반환"""
    try:
        query_params = None
        if params:
            param_list = json.loads(params)
            from datetime import datetime
            query_params = tuple(datetime.fromisoformat(p) if isinstance(p, str) and 'T' in p else p for p in param_list)
        
        df = execute_query(sql_query, query_params)
        
        result = {
            "success": True,
            "row_count": len(df),
            "columns": list(df.columns) if not df.empty else [],
            "data": df.to_dict('records') if not df.empty else []
        }
        
        return json.dumps(result, ensure_ascii=False, default=str)
        
    except Exception as e:
        print(f"MCP 실행 실패: {str(e)}")
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)

if __name__ == "__main__":
    print("🚀 BGP Analysis MCP 서버 시작 (포트: 8001)")
    print("📊 제공 도구:")
    print("  1. get_bgp_schema - BGP 테이블 스키마 및 개념 제공")
    print("  2. execute_bgp_query - SQL 쿼리 실행")
    print("🧠 MCP 클라이언트가 BGP 네트워크 분석 전문가 역할 수행!")
    
    mcp.run(transport="http", host="0.0.0.0", port=8001)