#!/usr/bin/env python3
from typing import List, Dict
from ollama import chat

class ReportGenerator:
    def __init__(self):
        self.system_prompt = (
            "You are a BGP anomaly detection expert. Only use the provided reports; do not add any additional information. "
            "All time expressions in user queries are based on Korean Standard Time (UTC+9). "
            "Convert them internally to UTC when reasoning about report timestamps. "
            "Please answer in English, and format the report clearly with headings, bullet points, and structured sections."
        )

    def generate_report(self, context: str, hits: List[Dict], query: str) -> str:
        """보고서 생성"""
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": f"{context}\n\nUser query: {query}"}
        ]

        try:
            response = chat(model="llama3:latest", messages=messages)
            # 응답에서 content만 추출
            if isinstance(response, dict) and 'message' in response:
                return response['message']['content']
            return str(response)
        except Exception as e:
            return f"Error generating report: {str(e)}"

    def check_deep_analysis_needed(self, hits: List[Dict]) -> bool:
        """심층 분석 필요 여부 확인"""
        # 결과가 없거나, 결과가 제한적인 경우 심층 분석 제안
        return len(hits) == 0 or any(hit['score'] < 0.7 for hit in hits) 