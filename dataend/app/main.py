from fastapi import FastAPI, Query
from elasticsearch import Elasticsearch

app = FastAPI()
es = Elasticsearch("http://elasticsearch:9200")
INDEX_NAME = "products"

@app.get("/")
def root():
    return "FastAPI Is Running!"

@app.get("/search/")
def search_product(product_name: str = Query(...)):
    query = {
        "query": {
            "multi_match": {
                "query": product_name,
                "fields": [
                    "product_name^4",
                    "product_name.edge^3",
                    "product_name.ngram^2",
                    "product_name.reverse^1"
                ],
                "fuzziness": "AUTO",
                "type": "most_fields"
            }
        },
        "_source": ["product_name", "category_name"],
        "size": 1
    }

    res = es.search(index=INDEX_NAME, body=query)

    if res["hits"]["hits"]:
        top = res["hits"]["hits"][0]["_source"]
        return {
            "product_name": top["product_name"],
            "category_name": top["category_name"]
        }
    else:
        return {"product_name": "제품명 없음", "category_name": "카테고리 없음"}