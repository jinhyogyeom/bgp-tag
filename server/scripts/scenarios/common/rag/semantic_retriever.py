#!/usr/bin/env python3
import os
from typing import List, Dict, Optional, Tuple
from sentence_transformers import SentenceTransformer
from pymilvus import connections, Collection
from .report_loader import ReportMetadata

class SemanticRetriever:
    def __init__(self, index_file: str, embedding_model: str):
        self.embedding_model = SentenceTransformer(embedding_model)
        self.collection = self._connect_milvus()

    def _connect_milvus(self) -> Collection:
        """Milvus 연결 및 컬렉션 로드"""
        try:
            connections.connect(
                alias="default",
                host=os.getenv('MILVUS_HOST', 'milvus'),
                port=os.getenv('MILVUS_PORT', '19530')
            )
            collection = Collection("bgp_reports")
            collection.load()
            return collection
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Milvus: {str(e)}")

    def retrieve(
        self,
        query: str,
        k: int,
        scenario_filter: Optional[str] = None,
        time_range: Optional[Tuple[str, str]] = None
    ) -> Tuple[str, List[Dict]]:
        """의미론적 검색 수행"""
        # 쿼리 임베딩
        query_embedding = self.embedding_model.encode([query], convert_to_numpy=True).astype('float32')
        
        # 검색 파라미터 설정
        search_params = {
            "metric_type": "L2",
            "params": {"nprobe": 10}
        }
        
        # 필터 조건 설정
        filter_conditions = []
        if scenario_filter:
            filter_conditions.append(f'scenario_type == "{scenario_filter}"')
        if time_range:
            start_time, end_time = time_range
            filter_conditions.append(f'timestamp >= "{start_time}" and timestamp <= "{end_time}"')
        
        filter_expr = " and ".join(filter_conditions) if filter_conditions else None
        
        # 검색 수행
        results = self.collection.search(
            data=query_embedding.tolist(),
            anns_field="embedding",
            param=search_params,
            limit=k,
            expr=filter_expr,
            output_fields=["timestamp", "scenario_type", "report"]
        )
        
        # 결과 포맷팅
        selected_meta = []
        filtered_texts = []
        
        for hit in results[0]:
            report = hit.entity.get('report')
            scenario_type = hit.entity.get('scenario_type')
            timestamp = hit.entity.get('timestamp')
            
            # 메타데이터 매핑
            meta = {
                'scenario_type': scenario_type or 'unknown',
                'timestamp': timestamp or '',
                'score': float(hit.distance)
            }
            
            selected_meta.append(meta)
            filtered_texts.append(report or '')
        
        context = "\n\n".join(filtered_texts)
        return context, selected_meta 