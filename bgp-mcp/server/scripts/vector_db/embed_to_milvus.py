#!/usr/bin/env python3
import json
import logging
import os
from datetime import datetime
from pymilvus import (
    connections,
    Collection,
    FieldSchema,
    CollectionSchema,
    DataType,
    utility,
)
from sentence_transformers import SentenceTransformer


BASE_PATH = os.getenv("BASE_PATH")

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ReportEmbedder:
    def __init__(self, embedding_dim=384):
        self.embedding_dim = embedding_dim
        # Milvus 연결 설정
        self.TARGET_DATE = os.getenv("TARGET_DATE")

        # Milvus 연결
        connections.connect(
            alias="default",
            host=os.getenv("MILVUS_HOST"),
            port=os.getenv("MILVUS_PORT"),
        )

        # SentenceTransformer 모델 로드
        self.model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

        # 컬렉션 생성
        self.create_collection()

    def create_collection(self):
        collection_name = f"bgp_reports_{self.TARGET_DATE}"

        # 컬렉션이 이미 존재하는지 확인
        if utility.has_collection(collection_name):
            logger.info(f"Collection {collection_name} already exists. Deleting...")
            Collection(collection_name).drop()
            logger.info(f"Deleted collection {collection_name}")

        # 필드 정의
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="timestamp", dtype=DataType.VARCHAR, max_length=100),
            FieldSchema(name="scenario_type", dtype=DataType.VARCHAR, max_length=50),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(
                name="vector", dtype=DataType.FLOAT_VECTOR, dim=self.embedding_dim
            ),
        ]

        # 스키마 생성
        schema = CollectionSchema(fields=fields, description="BGP Report Embeddings")

        # 컬렉션 생성
        collection = Collection(name=collection_name, schema=schema)

        # 인덱스 생성
        index_params = {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 1024},
        }
        collection.create_index(field_name="vector", index_params=index_params)
        logger.info(
            f"Created collection {collection_name} with index (vector field, dim={self.embedding_dim})"
        )

    def embed_reports(self, report_files):
        collection = Collection(f"bgp_reports_{self.TARGET_DATE}")
        collection.load()

        for file_path in report_files:
            try:
                with open(file_path, "r") as f:
                    for line in f:
                        try:
                            data = json.loads(line.strip())
                            report_text = data.get("report", "")

                            if not report_text:
                                continue

                            # 임베딩 생성
                            embedding = self.model.encode(report_text)
                            if len(embedding) != self.embedding_dim:
                                logger.error(
                                    f"임베딩 차원 불일치: {len(embedding)} (예상: {self.embedding_dim})"
                                )
                                continue

                            # 데이터 삽입
                            insert_data = {
                                "timestamp": data.get(
                                    "timestamp", datetime.now().isoformat()
                                ),
                                "scenario_type": data.get("scenario_type", "unknown"),
                                "text": report_text,
                                "vector": embedding.tolist(),
                            }

                            collection.insert([insert_data])
                            logger.info(
                                f"Successfully embedded report from {file_path}"
                            )

                        except json.JSONDecodeError as e:
                            logger.error(f"Error parsing JSON from {file_path}: {e}")
                            continue
                        except Exception as e:
                            logger.error(
                                f"Error processing report from {file_path}: {e}"
                            )
                            continue

                # 파일 내용 삭제
                with open(file_path, "w") as f:
                    f.write("")
                logger.info(f"Cleared contents of {file_path}")

            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
                continue

        collection.release()


def main():
    try:
        # 임베딩할 리포트 파일들
        report_files = [
            f"{BASE_PATH}/flap_10min_nl_reports.jsonl",
            f"{BASE_PATH}/hijack_10min_nl_reports.jsonl",
            f"{BASE_PATH}/loop_10min_nl_reports.jsonl",
            f"{BASE_PATH}/moas_10min_nl_reports.jsonl",
        ]

        embedder = ReportEmbedder()
        embedder.embed_reports(report_files)
        logger.info("Successfully completed embedding process")

    except Exception as e:
        logger.error(f"Error in embedding process: {e}")
        raise


if __name__ == "__main__":
    main()
