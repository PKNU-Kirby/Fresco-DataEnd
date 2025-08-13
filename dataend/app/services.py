import json
import pandas as pd
from openai import OpenAI
import os, base64, json

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def build_remap_prompt(input_name: str, es_result: str, df: pd.DataFrame) -> str:
    candidate_names = df["ingredientName"].drop_duplicates().tolist()
    
    prompt = (
    f"사용자가 입력한 식재료명과 Elasticsearch에서 매핑된 결과가 있습니다.\n"
    f"- 사용자 입력: {input_name}\n"
    f"- Elasticsearch 결과: {es_result}\n\n"
    f"만약 Elasticsearch 결과가 '기타'라면, 사용자의 원래 입력값을 참고해서 "
    f"아래 후보 리스트에서 가장 적합한 식재료명을 다시 골라주세요.\n\n"
    f"입력이 식재료가 아니거나 '물건'이라면 반드시 {{\"ingredientName\": \"제외\"}}로 답해주세요.\n\n"
    f"후보 리스트:\n{candidate_names}\n\n"
    f"반드시 아래 예시처럼 JSON 형식으로 답변하세요. 다른 설명은 하지 마세요.\n"
    f'예시: {{"ingredientName": "토마토"}}'
    )
    return prompt

def ask_openai_for_remap(input_name: str, es_result: str, df: pd.DataFrame) -> dict:
    prompt = build_remap_prompt(input_name, es_result, df)

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0
        )
        content = response.choices[0].message.content.strip()
        parsed = json.loads(content)
        matched_name = parsed.get("ingredientName", "기타")

        if matched_name == "제외":
            return {
                "input_name": input_name,
                "ingredientId": -1,
                "ingredientName": "제외",
                "categoryId": -1,
                "categoryName": "제외"
            }

        if matched_name in df["ingredientName"].values:
            match_row = df[df["ingredientName"] == matched_name].iloc[0]
            return {
                "input_name": input_name,
                "ingredientId": int(match_row["ingredientId"]),
                "ingredientName": match_row["ingredientName"],
                "categoryId": int(match_row["categoryId"]),
                "categoryName": match_row["categoryName"]
            }

    except Exception:
        pass
    
    return {
        "input_name": input_name,
        "ingredientId": 0,
        "ingredientName": "기타",
        "categoryId": 0,
        "categoryName": "기타"
    }

def ask_openai_for_detect(image_bytes: bytes, df: pd.DataFrame) -> list[dict]:
    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    candidate_names = df["ingredientName"].drop_duplicates().tolist()
    candidates_str = ", ".join(candidate_names)

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                f"아래 목록 중 이미지에 포함된 식재료를 모두 골라서, "
                                f"반드시 코드블록 없이 순수 JSON 배열 형태로 반환하세요. "
                                f"각 항목은 반드시 큰따옴표를 사용하는 JSON 객체이고, "
                                f"형태는 다음과 같습니다:\n"
                                f'[{{"ingredientName": "토마토"}}, {{"ingredientName": "감자"}}]\n\n'
                                f"목록: {candidates_str}"
                            )
                        },
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{image_b64}"}
                        }
                    ]
                }
            ],
            temperature=0.0
        )

        content = response.choices[0].message.content.strip()
        print("Raw OpenAI response content:", repr(content))
        parsed = json.loads(content)

        results = []
        
        for item in parsed:
            name = item.get("ingredientName", "").strip()
            if name in df["ingredientName"].values:
                row = df[df["ingredientName"] == name].iloc[0]
                results.append({
                    "ingredientId": int(row["ingredientId"]),
                    "ingredientName": row["ingredientName"],
                    "categoryId": int(row["categoryId"]),
                    "categoryName": row["categoryName"]
                })
        return results

    except Exception as e:
        print("OpenAI 호출 오류:", e)
        return []
    