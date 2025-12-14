import json
from typing import Any
from fastmcp import FastMCP
from query_execution import execute_query
from tools.system_instructions import build_system_instructions
from tools.schema import build_bgp_schema
from tools.sql_examples import build_sql_examples
from tools.prefix import resolve_prefix_lookup
from tools.statistics import (
    build_statistics_generation_payload,
    execute_statistics_code,
)

# FastMCP 서버 초기화
mcp = FastMCP(
    name="BGP Analysis Server",
    instructions="BGP 네트워크 데이터 분석 도구 제공 - 클라이언트가 전문가 역할 수행"
)

@mcp.tool()
def get_system_instructions() -> str:
    """BGP 분석 전문가 시스템 지침을 제공합니다."""
    return json.dumps(build_system_instructions(), ensure_ascii=False, indent=2)

@mcp.tool()
def get_bgp_schema() -> str:
    """BGP 데이터베이스 테이블 스키마 정보 제공"""
    return json.dumps(build_bgp_schema(), ensure_ascii=False, indent=2)

@mcp.tool()
def get_sql_examples() -> str:
    """BGP 분석을 위한 Few-shot 예제들을 제공합니다."""
    return json.dumps(build_sql_examples(), ensure_ascii=False, indent=2)

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


@mcp.tool()
def get_prefix(user_input: str) -> str:
    result = resolve_prefix_lookup(user_input)
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def statistics_code_generation(query_result: str, user_input: str) -> str:
    """DataFrame 정보와 사용자 요청을 분석하여 코드 생성에 필요한 정보를 제공합니다.
    
    MCP 클라이언트는 이 정보를 바탕으로 적절한 pandas 코드를 생성하세요.
    
    Args:
        query_result: execute_bgp_query 결과 (JSON 문자열)
        user_input: 사용자 자연어 입력 (예: "AS별 이벤트 수 집계", "시간대별 분포")
    
    Returns:
        DataFrame 스키마, 샘플 데이터, 사용자 요청 정보
    """
    payload = build_statistics_generation_payload(query_result, user_input)
    return json.dumps(payload, ensure_ascii=False, default=str)


@mcp.tool()
def execute_statistics(query_result: str, code: str) -> str:
    """생성된 pandas 코드를 DataFrame에 대해 실행합니다.
    
    Args:
        query_result: execute_bgp_query 결과 (JSON 문자열)
        code: statistics_code_generation 정보를 바탕으로 생성한 pandas 코드
              - df 변수로 DataFrame 접근
              - 결과는 반드시 result 변수에 저장
    
    Returns:
        코드 실행 결과 (JSON)
    """
    payload = execute_statistics_code(query_result, code)
    return json.dumps(payload, ensure_ascii=False, default=str)


@mcp.tool()
def get_bgpplay(input: str) -> str:
    """사용자 입력에서 특정 Prefix의 시간별 BGP 경로 변화 추이를 시각화하기 위한 start_time, end_time, prefix를 추출해 반환합니다
        start_time, end_time은 Timestamp 형식으로 반환
        ex :"resource": "193.0.0.0/21",
            "starttime": 1709251200,  // 2024-03-01 00:00:00 UTC
            "endtime": 1710460800,    // 2024-03-15 00:00:00 UTC
    
    Args:
        prefix: 조회할 프리픽스 (예: 1.0.0.0/24)
        start_time: 시작 시간 (ISO 형식)
        end_time: 종료 시간 (ISO 형식)
    
    Returns:
        prefix, start_time, end_time 형태의 JSON 배열
    """

    prefix, start_time, end_time = input.split(",")
    return json.dumps({"prefix": prefix, "start_time": start_time, "end_time": end_time}, ensure_ascii=False)


@mcp.tool()
def get_visualize(df: Any, user_input: str) -> str:
    """데이터프레임을 사용자 요구 방식대로 시각화하는 html/css/javascript 코드를 반환합니다.

    Args:
        df: 시각화할 데이터프레임
        user_input: 차트 유형 및 옵션이 담긴 사용자 입력 (예: chart_type, columns, 색상 등)

    Returns:
        시각화 html/css/javascript 코드
    """
    # TODO: 세부 구현 필요
    return json.dumps({"html_code": html_code, "css_code": css_code, "javascript_code": javascript_code}, ensure_ascii=False)
    html_code = f"""
    <html>
    <body>
    <h1>Hello, World!</h1>
    </body>
    </html>
    """

if __name__ == "__main__":
    print("🚀 BGP Analysis MCP 서버 시작 (포트: 8001)")
    print("📊 제공 도구:")
    print("  1. get_bgp_schema - BGP 테이블 스키마 및 개념 제공")
    print("  2. execute_bgp_query - SQL 쿼리 실행")
    print("  3. get_prefix - 한국 기관/기업의 AS 번호 목록 조회")
    print("  4. analyze_statistics - 데이터에 대한 통계 분석")
    print("  5. get_bgpplay - 특정 Prefix의 시간별 BGP 경로 변화 추이 시각화")
    print("  6. get_visualize - 데이터프레임을 사용자 요구 방식대로 시각화")
    print("🧠 MCP 클라이언트가 BGP 네트워크 데이터 분석 Ochestrator 역할 수행!")
    
    mcp.run(transport="http", host="0.0.0.0", port=8001)
