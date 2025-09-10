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