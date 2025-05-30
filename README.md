### 명령어
```sh
cd server
python -m uvicorn main:app --reload
```

### Docker
```sh
docker run -d \
	--name bgpdev \
	--network bgp_network \
	-v ./bgpdev_volume:/app \
	python:slim

docker run -d \
  --name bgpdb \
  --network bgp_network \
  -e POSTGRES_USER=bgp \
  -e POSTGRES_PASSWORD=bgp \
  -e POSTGRES_DB=bgpdb \
  -v ./bgpdb_volume:/var/lib/postgresql/data \
  -p 5432:5432 \
  timescale/timescaledb:latest-pg17

docker run -d \
  --name etcd \
  --network bgp_network \
  -e ETCD_AUTO_COMPACTION_MODE=revision \
  -e ETCD_AUTO_COMPACTION_RETENTION=1000 \
  -e ETCD_QUOTA_BACKEND_BYTES=4294967296 \
  -e ETCD_SNAPSHOT_COUNT=50000 \
  -v ./etcd_volume:/etcd \
  quay.io/coreos/etcd:v3.5.5 \
  etcd -advertise-client-urls=http://etcd:2379 -listen-client-urls=http://0.0.0.0:2379 --data-dir /etcd

docker run -d \
  --name minio \
  --network bgp_network \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -v ./minio_volume:/minio_data \
  -p 9000:9000 \
  minio/minio:RELEASE.2023-03-20T20-16-18Z \
  server /minio_data

docker run -d \
  --name milvus \
  --network bgp_network \
  -e ETCD_ENDPOINTS=etcd:2379 \
  -e MINIO_ADDRESS=minio:9000 \
  -e MINIO_ACCESS_KEY=minioadmin \
  -e MINIO_SECRET_KEY=minioadmin \
  -p 19530:19530 \
  -p 9091:9091 \
  -v ./milvus_volume:/var/lib/milvus \
  milvusdb/milvus:v2.3.3 \
  milvus run standalone
```

