FROM python:3.11-slim
LABEL maintainer="BGP Streamer"

# 작업 디렉토리 설정
WORKDIR /app

# Python 패키지 설치
RUN pip install --no-cache-dir clickhouse-driver websocket-client

COPY rislive_stream.py .

# 실행
ENTRYPOINT ["python3"]
CMD ["rislive_stream.py"]