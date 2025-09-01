# 1) CAIDA 공식 BGPStream 이미지 (libBGPStream + PyBGPStream 포함)
FROM caida/bgpstream:latest

# 2) 시스템 패키지 설치 (libbgpstream-dev 등은 필요 없습니다)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3.11-venv \
      python3.11-dev \
      python3-pip \
      libpq-dev \
      build-essential \
      gcc \
 && rm -rf /var/lib/apt/lists/*

# 3) Python venv 생성 및 활성화
RUN python3.11 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# 4) pip 업그레이드
RUN pip install --upgrade pip setuptools wheel

# 5) requirements.txt 복사 및 설치 (pybgpstream 제거된 상태)
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6) 애플리케이션 코드 복사
COPY . /app/

# 7) 실행 명령어 설정
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
