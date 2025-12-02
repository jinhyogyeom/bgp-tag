import requests
import json

# 루프 평가용 10문항
questions = [
    "2021년 10월 25일 하루 동안 발견된 루프 이벤트의 총 발생 횟수를 알려줘.",
    "2021-10-25 01:00:00 ~ 02:00:00 사이에 반복된 AS 번호가 있었는지, 어떤 AS인지 확인해줘.",
    "2021년 10월 25일과 26일 이틀 동안 루프가 가장 자주 발생한 프리픽스는 무엇인지 알려줘.",
    "2021-10-25 07:15:00 ~ 07:45:00 구간에 특정 AS 경로에서 루프가 발생했는지 사례를 나열해줘.",
    "2021년 10월 25일 하루 동안 루프 발생 평균 지속 시간은 얼마였는지 계산해줘.",
    "2021-10-26 10:00:00 ~ 11:00:00 구간에서 발생한 루프 이벤트들의 반복된 AS 번호를 알려줘.",
    "2021년 10월 26일 하루 동안 루프가 가장 많이 발생한 시간대는 언제인지 알려줘.",
    "2021-10-25 15:00:00 ~ 18:00:00 사이 특정 프리픽스에서 루프가 다수 관찰되었는지 확인해줘.",
    "2021년 10월 25일과 26일 이틀 동안 루프 이벤트로 인해 경로 길이가 비정상적으로 늘어난 사례가 있었는지 알려줘.",
    "2021-10-26 22:00:00 ~ 23:59:59 구간에 루프가 탐지된 AS_PATH를 모두 재현해줘."
]

ANS_FILE = "loop_model_answers.jsonl"
INVOKE_URL = "http://localhost:8080/invoke"

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

def call_invoke(q: str) -> dict:
    payload = {"messages": q}
    try:
        resp = session.post(INVOKE_URL, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        # 서비스 응답이 {"response": "...", "success": true, "error": null} 라고 가정
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