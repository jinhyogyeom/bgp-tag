import pandas as pd
import json

def generate_response(query: str, df: pd.DataFrame) -> str:
    """쿼리 결과를 자연어 응답으로 변환"""
    if df.empty:
        return "❌ 해당 조건에 맞는 데이터를 찾을 수 없습니다."
    
    # 개수 조회인 경우
    if len(df.columns) == 1 and 'count' in df.columns:
        count = df['count'].iloc[0]
        return f"📊 총 {count}개의 이벤트가 발견되었습니다."
    
    # 일반 데이터 결과
    response = f"📈 쿼리 결과 ({len(df)}개 레코드):\n\n"
    
    # 처음 5개 행만 표시
    for i, (_, row) in enumerate(df.head(5).iterrows(), 1):
        response += f"{i}. "
        for col, val in row.items():
            response += f"{col}: {val}, "
        response = response.rstrip(", ") + "\n"
    
    if len(df) > 5:
        response += f"... (총 {len(df)}개 중 처음 5개만 표시)\n"
    
    return response

def generate_insights(df: pd.DataFrame) -> str:
    """BGP 배경지식을 바탕으로 간단한 인사이트 제공"""
    if df.empty:
        return ""
    
    insights = []
    
    # BGP 이상 탐지 관련 기본 설명
    if 'event_type' in df.columns:
        event_types = df['event_type'].unique()
        for event_type in event_types:
            if event_type == 'origin_hijack':
                insights.append("💡 Origin Hijack: 프리픽스의 원래 AS가 아닌 다른 AS에서 광고하는 이상 현상")
            elif event_type == 'moas':
                insights.append("💡 MOAS (Multiple Origin AS): 하나의 프리픽스를 여러 AS에서 동시에 광고하는 현상")
            elif event_type == 'subprefix_hijack':
                insights.append("💡 Subprefix Hijack: 더 구체적인 서브넷을 광고하여 트래픽을 가로채는 공격")
    
    if 'as_path' in df.columns:
        insights.append("💡 AS Path: BGP 라우팅에서 패킷이 지나가는 AS들의 경로")
    
    if insights:
        return "\n" + "\n".join(insights)
    
    return ""