from openai import OpenAI
from elasticsearch import AsyncElasticsearch
from dotenv import load_dotenv
from sqlalchemy import create_engine

import pandas as pd
import os
import json
import asyncio

load_dotenv()

engine = create_engine(
    f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
es = AsyncElasticsearch("http://elasticsearch:9200")

query = """
        SELECT i.id as ingredientId,
            i.name as ingredientName,
            i.updatedAt,
            c.id as categoryId,
            c.name as categoryName
        FROM ingredients i
        JOIN categories c ON i.categoryId = c.id
    """

df = pd.read_sql(query, engine)

candidate_names = df["ingredientName"].drop_duplicates().tolist()

system_prompt = f"후보 식재료 리스트:\n{candidate_names}"

async def search_es(name, index_name):
    query = {
            "query": {
                    "bool": {
                        "should": [
                            {"match": {"ingredientName_nospace": {"query": name, "boost": 5}}},
                            {"match": {"ingredientName.edge": {"query": name, "boost": 3}}},
                            {"match": {"ingredientName": {"query": name, "boost": 2, "fuzziness": "AUTO"}}}
                        ]
                    }
                },
            "_source": ["ingredientId", "ingredientName", "categoryName", "categoryId"],
            "size": 1
        }
    
    res = await es.search(index=index_name, body=query)
    
    if res["hits"]["hits"]:
        return name, res["hits"]["hits"][0]["_source"]["ingredientName"]
    
    return name, "기타"

async def remap_wrapper(name, top_name):
    return await ask_openai_for_remap(name, top_name)


async def ask_openai_for_remap(input_name: str, es_result: str) -> dict:
    user_prompt = (
        f"사용자 입력: {input_name}\n"
        f"Elasticsearch 결과: {es_result}\n\n"
        f"{input_name}이 후보 리스트에 없거나 매핑이 되어 있어도 식재료가 아닌 물건일 경우 '제외'로 처리하고, '기타'이거나 다르게 매핑되어 있는 경우에도 "
        f"다시 한번 확인해서 가장 적합한 ingredientName을 JSON 형식으로 리턴하세요. 최대한 정확하고 제대로 매핑되도록 판단해주세요.\n"
        f'예시: {{"ingredientName": "토마토"}}'
    )

    try:
        response = await asyncio.to_thread(
            lambda: client.chat.completions.create(
                model="gpt-4.1-nano",
                messages=[{"role": "system", "content": system_prompt}, 
                          {"role": "user", "content": user_prompt}
                          ],
                temperature=0.0
            )
        )
        
        content = response.choices[0].message.content.strip()
        parsed = json.loads(content)
        
        print(parsed)
        
        matched_name = parsed.get("ingredientName", "기타").strip()
        
        print(matched_name)

        if matched_name == "제외":
            return {
                "input_name": input_name,
                "ingredientId": -1,
                "ingredientName": "제외",
                "categoryId": -1,
                "categoryName": "제외"
            }

        row = df[df["ingredientName"] == matched_name]
        
        if not row.empty:
            row = row.iloc[0]
            
            return {
                "input_name": input_name,
                "ingredientId": int(row["ingredientId"]),
                "ingredientName": row["ingredientName"],
                "categoryId": int(row["categoryId"]),
                "categoryName": row["categoryName"]
            }

    except Exception as e:
        print(f"OpenAI remap failed for {input_name}: {e}")

    return {
        "input_name": input_name,
        "ingredientId": 0,
        "ingredientName": "기타",
        "categoryId": 0,
        "categoryName": "기타"
    }