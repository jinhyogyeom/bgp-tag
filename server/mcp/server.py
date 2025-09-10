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
                    "event_type": "TEXT - origin_hijack/moas/subprefix_hijack",
                    "baseline_origin": "INTEGER - 기존 Origin AS",
                    "hijacker_origin": "INTEGER - 하이재커 Origin AS",
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
                    "as_path": "INTEGER[] - AS Path 배열",
                    "summary": "TEXT - 분석 요약"
                }
            },
            "flap_analysis_results": {
                "description": "프리픽스 플래핑 분석 결과",
                "columns": {
                    "time": "TIMESTAMPTZ - 이벤트 발생 시간",
                    "prefix": "TEXT - 플래핑된 프리픽스",
                    "peer_as": "INTEGER - Peer AS 번호",
                    "flap_count": "INTEGER - 플래핑 횟수",
                    "summary": "TEXT - 분석 요약"
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
                "question": "최근 24시간 동안 발생한 하이재킹 이벤트를 보여주세요",
                "sql": "SELECT * FROM hijack_events WHERE time >= NOW() - INTERVAL '24 hours' ORDER BY time DESC LIMIT 10;",
                "explanation": "최근 24시간의 하이재킹 이벤트를 시간 역순으로 조회"
            },
            {
                "question": "특정 AS(예: AS12345)와 관련된 이벤트를 찾아주세요",
                "sql": "SELECT * FROM hijack_events WHERE baseline_origin = 12345 OR hijacker_origin = 12345 ORDER BY time DESC;",
                "explanation": "AS12345가 피해자이거나 가해자인 하이재킹 이벤트 조회"
            },
            {
                "question": "Origin Hijack 이벤트만 필터링해서 보여주세요",
                "sql": "SELECT * FROM hijack_events WHERE event_type = 'origin_hijack' ORDER BY time DESC LIMIT 20;",
                "explanation": "Origin Hijack 타입의 이벤트만 조회"
            },
            {
                "question": "가장 많은 플래핑이 발생한 프리픽스 Top 5를 보여주세요",
                "sql": "SELECT prefix, MAX(flap_count) as max_flaps FROM flap_analysis_results GROUP BY prefix ORDER BY max_flaps DESC LIMIT 5;",
                "explanation": "프리픽스별 최대 플래핑 횟수를 집계하여 상위 5개 조회"
            },
            {
                "question": "AS Path에 루프가 있는 이벤트의 개수를 세어주세요",
                "sql": "SELECT COUNT(*) as loop_count FROM loop_analysis_results;",
                "explanation": "AS Path 루프 이벤트의 총 개수 조회"
            },
            {
                "question": "특정 프리픽스(예: 1.0.0.0/24)와 관련된 모든 이벤트를 찾아주세요",
                "sql": "SELECT * FROM hijack_events WHERE prefix = '1.0.0.0/24' ORDER BY time DESC;",
                "explanation": "특정 프리픽스와 관련된 모든 하이재킹 이벤트 조회"
            }
        ],
        "sql_patterns": {
            "time_filtering": "WHERE time >= NOW() - INTERVAL '24 hours'",
            "ordering": "ORDER BY time DESC",
            "limiting": "LIMIT 10",
            "counting": "SELECT COUNT(*) as count FROM table_name",
            "grouping": "GROUP BY column_name ORDER BY count DESC",
            "event_type_filter": "WHERE event_type = 'origin_hijack'",
            "as_filtering": "WHERE baseline_origin = AS_NUMBER OR hijacker_origin = AS_NUMBER"
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
        return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False)

if __name__ == "__main__":
    print("🚀 BGP Analysis MCP 서버 시작 (포트: 8001)")
    print("📊 제공 도구:")
    print("  1. get_bgp_schema - BGP 테이블 스키마 및 개념 제공")
    print("  2. execute_bgp_query - SQL 쿼리 실행")
    print("🧠 MCP 클라이언트가 BGP 네트워크 분석 전문가 역할 수행!")
    
    mcp.run(transport="http", host="0.0.0.0", port=8001)