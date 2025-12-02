#!/usr/bin/env python3
import argparse
import os
import sys
import json
from typing import List, Dict, Optional, Tuple
from .report_loader import ReportLoader
from .semantic_retriever import SemanticRetriever
from .report_generator import ReportGenerator

class BGPReportRetriever:
    def __init__(
        self,
        report_file: str = None,
        index_file: str = None,
        meta_file: str = None,
        embedding_model: str = 'all-MiniLM-L6-v2'
    ):
        self.retriever = SemanticRetriever(None, embedding_model)
        self.generator = ReportGenerator()

    def retrieve_reports(
        self,
        query: str,
        top_k: int = 5,
        scenario_filter: Optional[str] = None,
        time_range: Optional[Tuple[str, str]] = None
    ) -> Dict:
        """보고서 검색 및 생성"""
        # 1. 보고서 검색
        context, hits = self.retriever.retrieve(
            query=query,
            k=top_k,
            scenario_filter=scenario_filter,
            time_range=time_range
        )

        # 2. 보고서 생성
        report = self.generator.generate_report(context, hits, query)
        
        # report가 딕셔너리인 경우 문자열로 변환
        if isinstance(report, dict):
            report = str(report)

        # 3. 심층 분석 필요 여부 확인
        needs_deep_analysis = self.generator.check_deep_analysis_needed(hits)

        return {
            "report": report,
            "needs_deep_analysis": needs_deep_analysis,
            "hits": hits
        }

def main():
    parser = argparse.ArgumentParser(
        description="BGP anomaly reports retrieval system"
    )
    parser.add_argument('query', type=str, help="Natural language query")
    parser.add_argument('--top_k', type=int, default=5, help="Top-K reports to retrieve")
    parser.add_argument('--scenario', type=str, help="Filter by scenario type")
    parser.add_argument('--start_time', type=str, help="Start time for filtering")
    parser.add_argument('--end_time', type=str, help="End time for filtering")
    args = parser.parse_args()

    try:
        # 리트리버 초기화
        retriever = BGPReportRetriever(
            embedding_model=os.getenv('EMBEDDING_MODEL', 'all-MiniLM-L6-v2')
        )

        # 시간 범위 설정
        time_range = None
        if args.start_time and args.end_time:
            time_range = (args.start_time, args.end_time)

        # 보고서 검색 및 생성
        result = retriever.retrieve_reports(
            args.query,
            args.top_k,
            args.scenario,
            time_range
        )

        # 결과 출력
        print(json.dumps(result, indent=2))

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main() 