from fastapi import FastAPI, Query, File, UploadFile
from services import search_es, remap_wrapper, ask_openai_for_remap, ask_openai_for_detect

import pandas as pd
import asyncio

df = pd.read_csv("data/product_name_categories.csv")

app = FastAPI()

@app.get("/")
def root():
    return "FastAPI Is Running!"

@app.get("/search/")
async def search_products(ingredient_names: list[str] = Query(...)):
    
    es_tasks = [search_es(name, "products") for name in ingredient_names]
    es_results = await asyncio.gather(*es_tasks)
    
    for i in es_results:
        print(*i)

    remap_tasks = [remap_wrapper(name, top_name) for name, top_name in es_results]
    remap_results = await asyncio.gather(*remap_tasks)
    
    final_results = [r for r in remap_results if r and r["ingredientName"] != "제외"]
    
    return final_results

@app.post("/detect/")
async def detect_ingredients(image: UploadFile = File(...)):
    image_bytes = await image.read()
    
    results = ask_openai_for_detect(image_bytes, df)
    
    return results