import struct
from datetime import datetime, timezone


def read_mrt_header(file_path: str):
    with open(file_path, "rb") as f:
        header = f.read(12)
        if len(header) != 12:
            raise ValueError("파일의 헤더 길이가 12바이트가 아닙니다.")
        # 네트워크 바이트 오더(!): 4바이트 타임스탬프, 2바이트 타입, 2바이트 서브타입, 4바이트 레코드 길이
        timestamp, mrt_type, mrt_subtype, record_length = struct.unpack("!IHHI", header)
        return timestamp, mrt_type, mrt_subtype, record_length


def main(file_path: str):
    try:
        timestamp, mrt_type, mrt_subtype, record_length = read_mrt_header(file_path)
        # 타임스탬프를 사람이 읽을 수 있는 형태로 변환 (UTC 기준)
        dt = datetime.fromtimestamp(timestamp, timezone.utc)
        print("타임스탬프:", timestamp, "->", dt.strftime("%Y-%m-%d %H:%M:%S UTC"))
        print("MRT 타입:", mrt_type)
        print("MRT 서브타입:", mrt_subtype)
        print("레코드 길이:", record_length)
    except Exception as e:
        print("에러 발생:", e)


if __name__ == "__main__":
    main("./routeviews_data/rib.20250101.0000")
