import csv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

es = Elasticsearch("http://elasticsearch:9200", request_timeout=60, max_retries=3, retry_on_timeout=True)
INDEX_NAME = "products"

mapping = {
    "settings": {
        "index": {
            "max_ngram_diff": 19,
            "routing": {
                "allocation": {
                    "include": {
                        "_tier_preference": "data_content"
                    }
                }
            },
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
                "reverse_search_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "reverse"]
                },
                "edge_ngram_analyzer": {
                    "type": "custom",
                    "tokenizer": "edge_ngram_tokenizer",
                    "filter": ["lowercase"]
                },
                "ngram_analyzer": {
                    "type": "custom",
                    "tokenizer": "ngram_tokenizer",
                    "filter": ["lowercase"]
                },
                "reverse_edge_ngram_analyzer": {
                    "type": "custom",
                    "tokenizer": "reverse_edge_ngram_tokenizer",
                    "filter": ["lowercase", "reverse"]
                },
                "default_search_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                }
            },
            "tokenizer": {
                "reverse_edge_ngram_tokenizer": {
                    "type": "edge_ngram",
                    "min_gram": 1,
                    "max_gram": 20,
                    "token_chars": ["letter", "digit", "whitespace"]
                },
                "edge_ngram_tokenizer": {
                    "type": "edge_ngram",
                    "min_gram": 1,
                    "max_gram": 20,
                    "token_chars": ["letter", "digit", "whitespace"]
                },
                "ngram_tokenizer": {
                    "type": "ngram",
                    "min_gram": 1,
                    "max_gram": 20,
                    "token_chars": ["letter", "digit", "whitespace"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "product_name": {
                "type": "text",
                "analyzer": "edge_ngram_analyzer",
                "search_analyzer": "default_search_analyzer",
                "fields": {
                    "keyword": {"type": "keyword"},
                    "ngram": {
                        "type": "text",
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "default_search_analyzer"
                    },
                    "reverse": {
                        "type": "text",
                        "analyzer": "reverse_edge_ngram_analyzer",
                        "search_analyzer": "reverse_search_analyzer"
                    }
                }
            },
            "category_name": {
                "type": "keyword"
            }
        }
    }
}

def create_index_if_not_exists():
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, body=mapping)

def bulk_index_from_csv(csv_path):
    actions = []
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            product_name = row.get("product_name")
            category_name = row.get("category_name")
            if not product_name:
                continue

            action = {
                "_index": INDEX_NAME,
                "_id": i + 1,
                "_source": {
                    "product_name": product_name.strip(),
                    "category_name": (category_name or "").strip()
                }
            }
            actions.append(action)

    bulk(es, actions)
    es.indices.refresh(index=INDEX_NAME)

if __name__ == "__main__":
    if es.ping():
        print("Elasticsearch 서버 연결 성공")
    else:
        print("Elasticsearch 서버 연결 실패")
        
    create_index_if_not_exists()
    bulk_index_from_csv("/data/product_name_categories.csv")
    print("인덱스 생성 및 bulk 색인 완료.")
