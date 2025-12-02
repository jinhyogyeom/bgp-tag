## BGP 분석 시스템

### 실행
```bash
mkdir -p volumes/etcd volumes/minio
docker compose up -d
```

### 접속
```bash
# FastAPI: http://localhost:8000
# DB: postgres://postgres:postgres@localhost:5432/bgp_timeseries
# Milvus: localhost:19530
```

### 개발
```bash
# 컨테이너 접속
docker compose exec bgp-app bash

# 로그 확인
docker compose logs -f bgp-app

# 재시작
docker compose restart

# 중지
docker compose down
```

