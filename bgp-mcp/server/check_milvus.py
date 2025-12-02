from pymilvus import connections, Collection

# Milvus에 연결
connections.connect(host="milvus", port="19530")

# 조회할 컬렉션명
collection_name = "bgp_reports_20250525"

# 컬렉션 로드
collection = Collection(name=collection_name)
collection.load()

# 컬렉션 전체 데이터 조회
results = collection.query(
    expr="",
    output_fields=["id", "timestamp", "scenario_type", "text"],
    limit=10,  # 예시로 10개만 출력
)

# 출력
for item in results:
    print(item)

# 컬렉션 해제 (자원 해제)
collection.release()
