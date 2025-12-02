import requests
import json

# 플랩 평가용 10문항
questions = [
    "2021년 10월 25일 하루 동안 전체 네트워크에서 탐지된 플랩 이벤트의 총 발생 횟수를 알려줘.",
    "2021년 10월 25일 06:00:00 ~ 12:00:00 구간 동안 플랩 빈도가 가장 높은 AS 상위 5개를 알려줘.",
    "2021년 10월 25일과 26일 이틀 동안 플랩이 가장 많이 발생한 프리픽스 상위 5개를 알려줘.",
    "2021년 10월 25일 18시부터 20시까지 플랩이 급증한 AS 상위 5개를 알려줘.",
    "2021년 10월 25일과 26일 이틀 동안 203.0.113.0/24 경로에서 나타난 플랩 이벤트 패턴을 시간대별로 비교해줘.",
    "2021-10-25 14:05:05 ~ 14:25:25 사이 withdrawal 반복으로 인한 플랩이 특정 프리픽스에서 발생했는지 확인해줘.",
    "2021년 10월 26일 00시부터 06시까지 새벽 시간대에 발생한 플랩 이벤트들의 총 발생 횟수를 집계해줘.",
    "2021-10-26 09:30:00 ~ 10:30:00 구간 동안 AS64501의 경로가 반복적으로 변동된 사례를 알려줘.",
    "2021년 10월 26일 하루 동안 플랩 지속 시간이 가장 긴 이벤트는 언제 발생했는지 알려줘.",
    "2021-10-25 20:00:00 ~ 23:59:59 저녁 시간대에 플랩으로 인해 가장 심각한 불안정성을 보인 AS는 누구였는지 분석해줘."
]

ANS_FILE = "flap_model_answers.jsonl"
INVOKE_URL = "http://localhost:8080/invoke"

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

def call_invoke(q: str) -> dict:
    payload = {"messages": q}
    try:
        resp = session.post(INVOKE_URL, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        return {
            "input": q,
            "response": data.get("response", ""),
            "success": data.get("success", True),
            "error": data.get("error")
        }
    except requests.RequestException as e:
        return {"input": q, "response": "", "success": False, "error": str(e)}
    except ValueError:
        return {"input": q, "response": resp.text if 'resp' in locals() else "", "success": False, "error": "Invalid JSON from server"}

def main():
    wrote = 0
    with open(ANS_FILE, "w", encoding="utf-8") as f:
        for q in questions:
            rec = call_invoke(q)
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            wrote += 1
            if not rec["success"]:
                print(f"[WARN] 실패: {q} -> {rec['error']}")
            else:
                print(f"[OK] 수집: {q[:40]}...")

    print(f"✅ 모델 답변 수집 완료: {ANS_FILE} (총 {wrote}문항)")

if __name__ == "__main__":
    main()