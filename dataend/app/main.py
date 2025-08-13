from fastapi import FastAPI, Query, File, UploadFile
from elasticsearch import Elasticsearch
from openai import OpenAI
from services import build_remap_prompt, ask_openai_for_remap, ask_openai_for_detect
import os
import pandas as pd
import json
import base64

app = FastAPI()
es = Elasticsearch("http://elasticsearch:9200")
df = pd.read_csv("/data/product_name_categories.csv")
INDEX_NAME = "products"

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


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

@app.post("/detect/")
async def detect_ingredients(image: UploadFile = File(...)):
    image_bytes = await image.read()
    
    results = ask_openai_for_detect(image_bytes, df)
    
    return results