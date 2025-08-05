from fastapi import FastAPI, Query
from elasticsearch import Elasticsearch
from openai import OpenAI
import os
import pandas as pd
import json

app = FastAPI()
es = Elasticsearch("http://elasticsearch:9200")
df = pd.read_csv("/data/product_name_categories.csv")
INDEX_NAME = "products"

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def build_prompt(input_name: str, es_result: str, df: pd.DataFrame) -> str:
    candidate_names = df["ingredientName"].drop_duplicates().tolist()
    
    prompt = (
        f"다음은 사용자가 입력한 식재료명과 Elasticsearch에서 매핑된 결과입니다.\n"
        f"- 사용자 입력: {input_name}\n"
        f"- Elasticsearch 결과: {es_result}\n\n"
        f"만약 '{input_name}'이 '물건'이거나 식재료, 식품이 아닌 경우 반드시 '제외'처리해야 합니다. 최대한 식재료나 식품 등이 덜 걸러지도록 상세히 분석해주세요.\n"
        f'{{"ingredientName": "제외"}}\n\n'
        f"식재료명이 포함되어 있거나 그 외에는 가장 유사한 식재료명을 아래 리스트에서 골라주세요.\n\n"
        f"사용자가 입력한 식재료명을 분석하고 (특히 브랜드명 등) 가능한 가장 적절한 항목을 아래 리스트에서 골라주세요.\n\n"
        f"식자재 목록 (JSON 배열):\n{candidate_names}\n\n"
        f"반드시 JSON 형식으로 다음과 같이만 답변해 주세요. 다른 설명은 하지 마세요.\n"
        f'{{"ingredientName": "매핑된 식재료명"}}'
    )
    return prompt

def ask_openai_for_remap(input_name: str, es_result: str, df: pd.DataFrame) -> dict:
    prompt = build_prompt(input_name, es_result, df)

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
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

@app.get("/")
def root():
    return "FastAPI Is Running!"

@app.get("/search/")
def search_products(ingredient_names: list[str] = Query(...)):
    results = []
    for name in ingredient_names:
        query = {
            "query": {
                "multi_match": {
                    "query": name,
                    "fields": [
                        "ingredientName^4",
                        "ingredientName.edge^3",
                        "ingredientName.ngram^2",
                        "ingredientName.reverse^1",
                        "ingredientName.nospace^2",
                        "ingredientName_nospace^2"
                    ],
                    "fuzziness": "AUTO",
                    "type": "most_fields"
                }
            },
            "_source": ["ingredientId", "ingredientName", "categoryName", "categoryId"],
            "size": 1
        }

        res = es.search(index=INDEX_NAME, body=query)

        if res["hits"]["hits"]:
            top = res["hits"]["hits"][0]["_source"]
            final = ask_openai_for_remap(name, top["ingredientName"], df)
        else:
            final = ask_openai_for_remap(name, "기타", df)

        if final["ingredientName"] != "제외":
            results.append({
                "input_name": final.get("input_name", name),
                "ingredientId": final.get("ingredientId", 0),
                "ingredientName": final.get("ingredientName", "기타"),
                "categoryId": final.get("categoryId", 0),
                "categoryName": final.get("categoryName", "기타"),
            })

    return results