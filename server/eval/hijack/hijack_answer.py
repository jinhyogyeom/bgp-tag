import requests
import json

# 하이재킹 평가용 10문항
questions = [
    "2021년 10월 25일 00시부터 23시59분까지 전체 네트워크에서 MOAS 형태의 하이재킹이 감지된 프리픽스를 모두 나열해줘.",
    "2021-10-25 13:00:00 ~ 14:00:00 구간에 두 개 이상의 origin AS가 동일한 프리픽스를 동시에 광고한 MOAS 이벤트가 있었는지 알려줘.",
    "2021년 10월 25~26일 이틀간 MOAS 이벤트 중 peer 수가 20개 이상인 사례를 보여줘.",
    "2021-10-25 15:00:00 ~ 15:40:00 사이 23456이 포함된 MOAS 이벤트가 존재하는지 확인하고 다른 origin 목록을 함께 보여줘.",
    "2021년 10월 26일 하루 동안 MOAS 이벤트의 총 발생 건수와 상위 3개 프리픽스를 알려줘.",
    "2021-10-25 10:00:00 ~ 11:00:00 구간에 새로 등장한 origin AS(기존 baseline_origin과 달라진 경우)가 있었는지 찾아줘.",
    "2021년 10월 25일 하루 동안 origin_asns에 136910이 포함된 모든 하이재킹 이벤트를 보여줘.",
    "2021-10-26 08:00:00 ~ 09:00:00 구간에 발생한 MOAS 이벤트 중 이벤트 수(total_events)가 가장 많은 사례를 알려줘.",
    "2021년 10월 25~26일 동안 1시간 이상 지속된 MOAS 이벤트가 있었는지 확인해줘.",
    "2021-10-26 21:00:00 ~ 23:59:59 구간에 동일한 AS가 여러 프리픽스에서 동시 MOAS를 형성한 경우가 있는지 알려줘."
]

ANS_FILE = "hijack_model_answers.jsonl"
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

    print(f"✅ 하이재킹 모델 답변 수집 완료: {ANS_FILE} (총 {wrote}문항)")

if __name__ == "__main__":
    main()