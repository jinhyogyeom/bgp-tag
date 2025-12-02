from openai import OpenAI
import json
import os

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# -------------------------------
# 채점 프롬프트 구성
# -------------------------------
def build_prompt(question, ideal, answer):
    return f"""
[질문]
{question}

[정답지]
{json.dumps(ideal, ensure_ascii=False, indent=2)}

[모델 답변]
{answer}

[채점 규칙]
- 이벤트 종류: loop 만 정답으로 인정
- 평가 기준:
  1. loop 이벤트 인식 (0/2점)
  2. 시간 범위 반영 (0~3점)
  3. 수치 일치(event_count 등) (0~3점)
  4. 설명 품질 (0~2점)

[출력 형식]
JSON:
{{
  "이벤트종류": <0|2>,
  "시간범위": <0~3>,
  "수치일치": <0~3>,
  "설명품질": <0~2>,
  "총점": <0~10>
}}
"""

# -------------------------------
# 채점 함수
# -------------------------------
def grade_loop(question, ideal, answer, success=True):
    prompt = build_prompt(question, ideal, answer)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0
    )

    try:
        content = response.choices[0].message.content.strip()
        # 코드 블록 제거
        if content.startswith("```json"):
            content = content.replace("```json", "").replace("```", "").strip()
        elif content.startswith("```"):
            content = content.replace("```", "").strip()

        parsed = json.loads(content)

        # 총점은 이벤트종류(2) + 시간범위(3) + 수치일치(3) + 설명품질(2)
        parsed["총점"] = (
            parsed.get("이벤트종류", 0)
            + parsed.get("시간범위", 0)
            + parsed.get("수치일치", 0)
            + parsed.get("설명품질", 0)
        )

        return parsed

    except Exception as e:
        print("⚠️ 채점 파싱 실패:", e)
        print("원본 응답:", response.choices[0].message.content)
        return None


# -------------------------------
# 메인 루틴
# -------------------------------
if __name__ == "__main__":
    gt_file = "test3.jsonl"                 # 정답지 JSONL
    ans_file = "loop_model_answers.jsonl"   # LLM 답변 JSONL
    out_file = "loop_graded_results.jsonl"  # 결과 저장

    results = []

    # 정답지 로드
    with open(gt_file, "r", encoding="utf-8") as f:
        gt_data = [json.loads(line) for line in f]

    # 모델 답변 로드
    with open(ans_file, "r", encoding="utf-8") as f:
        ans_data = [json.loads(line) for line in f]

    # 한 문항씩 채점
    for gt, ans in zip(gt_data, ans_data):
        q = gt["input"]
        ideal = gt["ideal"]
        model_answer = ans.get("response", "")
        success_flag = ans.get("success", True)

        score = grade_loop(q, ideal, model_answer, success=success_flag)
        results.append({
            "input": q,
            "ideal": ideal,
            "model_answer": model_answer,
            "success": success_flag,
            "score": score
        })

    # 결과 저장
    with open(out_file, "w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"✅ 루프 평가 완료: {out_file}")