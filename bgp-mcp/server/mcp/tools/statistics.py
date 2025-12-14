import json
from typing import Any, Dict

import pandas as pd


def _parse_query_result(query_result: str) -> pd.DataFrame:
    parsed_data = json.loads(query_result)
    if isinstance(parsed_data, dict) and "data" in parsed_data:
        return pd.DataFrame(parsed_data["data"])
    return pd.DataFrame(parsed_data)


def build_statistics_generation_payload(query_result: str, user_input: str) -> Dict[str, Any]:
    """Create metadata for pandas code generation."""
    try:
        df = _parse_query_result(query_result)
    except json.JSONDecodeError as exc:
        return {"success": False, "error": f"JSON 파싱 오류: {str(exc)}"}

    if df.empty:
        return {"success": False, "error": "분석할 데이터가 비어있습니다."}

    schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
    sample_data = df.head(3).to_dict("records")

    return {
        "success": True,
        "user_input": user_input,
        "row_count": len(df),
        "columns": list(df.columns),
        "schema": schema,
        "sample_data": sample_data,
        "instruction": "위 정보를 바탕으로 user_input 요청에 맞는 pandas 코드를 생성하세요. 결과는 result 변수에 저장해야 합니다.",
    }


def execute_statistics_code(query_result: str, code: str) -> Dict[str, Any]:
    """Run pandas code against the DataFrame for post-processing."""
    try:
        df = _parse_query_result(query_result)
    except json.JSONDecodeError as exc:
        return {"success": False, "error": f"JSON 파싱 오류: {str(exc)}"}

    if df.empty:
        return {"success": False, "error": "분석할 데이터가 비어있습니다."}

    local_vars = {"df": df, "pd": pd, "result": None}
    try:
        exec(code, {"pd": pd, "json": json}, local_vars)
    except Exception as exc:
        return {"success": False, "error": f"코드 실행 오류: {str(exc)}"}

    result = local_vars.get("result")
    if result is None:
        return {"success": False, "error": "코드 실행 결과가 없습니다. result 변수에 결과를 저장하세요."}

    return {"success": True, "executed_code": code, "result": result}
