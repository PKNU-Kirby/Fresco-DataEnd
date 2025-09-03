from openai import OpenAI
from repositories import get_all_ingredients

import json
import os
import base64
import threading
import time

INDEX_NAME = "products"

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
df = get_all_ingredients()

def refresh_df_periodically(interval_seconds=60):
    global df
    while True:
        try:
            df = get_all_ingredients()
            print(f"df refreshed at {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
        except Exception as e:
            print(f"Failed to refresh df: {e}",  flush=True)
        time.sleep(interval_seconds)

threading.Thread(target=refresh_df_periodically, args=(60,), daemon=True).start()

def ask_openai_for_detect(image_bytes: bytes) -> list[dict]:
    global df   
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
    