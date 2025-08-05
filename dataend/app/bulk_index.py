import csv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

es = Elasticsearch("http://elasticsearch:9200", request_timeout=60, max_retries=3, retry_on_timeout=True)
INDEX_NAME = "products"

mapping = {
  "settings": {
    "index": {
      "max_ngram_diff": 18,
      "number_of_shards": 1,
      "number_of_replicas": 1
    },
    "analysis": {
      "filter": {
        "reverse": {
          "type": "reverse"
        }
      },
      "analyzer": {
        "korean_edge_ngram_analyzer": {
          "type": "custom",
          "tokenizer": "korean_edge_ngram_tokenizer"
        },
        "korean_ngram_analyzer": {
          "type": "custom",
          "tokenizer": "korean_ngram_tokenizer"
        },
        "korean_reverse_edge_ngram_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": ["reverse"]
        },
        "korean_standard_search_analyzer": {
          "type": "custom",
          "tokenizer": "standard"
        }
      },
      "tokenizer": {
        "korean_edge_ngram_tokenizer": {
          "type": "edge_ngram",
          "min_gram": 2,
          "max_gram": 20,
          "token_chars": ["letter", "digit", "whitespace"]
        },
        "korean_ngram_tokenizer": {
          "type": "ngram",
          "min_gram": 2,
          "max_gram": 20,
          "token_chars": ["letter", "digit", "whitespace"]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "ingredientId": { "type": "keyword" },
      "categoryId": { "type": "keyword" },
      "ingredientName": {
        "type": "text",
        "analyzer": "korean_edge_ngram_analyzer",
        "search_analyzer": "korean_standard_search_analyzer",
        "fields": {
          "keyword": {
            "type": "keyword"
          },
          "ngram": {
            "type": "text",
            "analyzer": "korean_ngram_analyzer",
            "search_analyzer": "korean_standard_search_analyzer"
          },
          "reverse": {
            "type": "text",
            "analyzer": "korean_reverse_edge_ngram_analyzer",
            "search_analyzer": "korean_standard_search_analyzer"
          },
          "edge": {
            "type": "text",
            "analyzer": "korean_edge_ngram_analyzer",
            "search_analyzer": "korean_standard_search_analyzer"
          },
          "nospace": {
            "type": "keyword"
          }
        }
      },
      "categoryName": { "type": "keyword" },
      "ingredientName_nospace": {
        "type": "keyword"
      }
    }
  }
}

def create_index_if_not_exists():
    if es.indices.exists(index=INDEX_NAME):
        print(f"인덱스 {INDEX_NAME} 존재하므로 삭제합니다.")
        es.indices.delete(index=INDEX_NAME)
    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"인덱스 {INDEX_NAME} 생성 완료")

def bulk_index_from_csv(csv_path):
    actions = []
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            ingredient_id = int(row["ingredientId"])
            category_id = int(row["categoryId"])
            ingredient_name = row["ingredientName"].strip()
            category_name = row["categoryName"].strip()
            nospace_name = ingredient_name.replace(" ", "")

            action = {
                "_index": INDEX_NAME,
                "_id": i + 1,
                "_source": {
                    "ingredientId": ingredient_id,
                    "ingredientName": ingredient_name,
                    "ingredientName_nospace": nospace_name,
                    "categoryName": category_name,
                    "categoryId": category_id
                }
            }
            actions.append(action)

    bulk(es, actions)
    es.indices.refresh(index=INDEX_NAME)
    print(f"{len(actions)}개 문서 색인 완료")

if __name__ == "__main__":
    if es.ping():
        print("Elasticsearch 서버 연결 성공")
    else:
        print("Elasticsearch 서버 연결 실패")
        exit(0)
        
    create_index_if_not_exists()
    bulk_index_from_csv("/data/product_name_categories.csv")
    print("인덱스 생성 및 bulk 색인 완료.")
