from elasticsearch import Elasticsearch
from datetime import datetime, timezone
from elasticsearch.helpers import bulk
from dotenv import load_dotenv
from pymysql.cursors import DictCursor

import os
import pymysql

INDEX_NAME = "products"

es = Elasticsearch("http://localhost:9200").options(request_timeout=60)

mapping = {
    "settings": {
        "index": {
            "max_ngram_diff": 20,
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "analysis": {
            "analyzer": {
                "korean_ngram_analyzer": {
                    "type": "custom",
                    "tokenizer": "korean_ngram_tokenizer"
                },
                "korean_edge_ngram_analyzer": {
                    "type": "custom",
                    "tokenizer": "korean_edge_ngram_tokenizer"
                }
            },
            "tokenizer": {
                "korean_ngram_tokenizer": {
                    "type": "ngram",
                    "min_gram": 2,
                    "max_gram": 20,
                    "token_chars": ["letter", "digit"]
                },
                "korean_edge_ngram_tokenizer": {
                    "type": "edge_ngram",
                    "min_gram": 1,
                    "max_gram": 20,
                    "token_chars": ["letter", "digit"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "ingredientId": {"type": "keyword"},
            "categoryId": {"type": "keyword"},
            "ingredientName": {
                "type": "text",
                "analyzer": "korean_ngram_analyzer",
                "search_analyzer": "korean_ngram_analyzer",
                "fields": {
                    "edge": {
                        "type": "text",
                        "analyzer": "korean_edge_ngram_analyzer",
                        "search_analyzer": "korean_edge_ngram_analyzer"
                    },
                    "keyword": {"type": "keyword"}
                }
            },
            "ingredientName_nospace": {
                "type": "text",
                "analyzer": "korean_ngram_analyzer",
                "search_analyzer": "korean_ngram_analyzer"
            },
            "categoryName": {"type": "keyword"},
            "updatedAt": {"type": "date"}
        }
    }
}

def create_index_if_not_exists():
    if es.indices.exists(index=INDEX_NAME):
        print(f"인덱스 '{INDEX_NAME}' 이미 존재")
        return False  # 이미 존재
    else:
        es.indices.create(index=INDEX_NAME, body=mapping)
        print(f"인덱스 '{INDEX_NAME}' 생성 완료")
        return True  # 새로 생성

def sync_to_es():
    # Elasticsearch 연결 확인
    if not es.ping():
        print("Elasticsearch 연결 실패")
        return

    # 인덱스 존재 여부 확인 및 생성
    is_new_index = create_index_if_not_exists()

    # 동기화 전 문서 수
    count_before = es.count(index=INDEX_NAME)['count'] if not is_new_index else 0
    print(f"동기화 전 문서 수: {count_before}")

    # MySQL 연결
    try:
        load_dotenv()
        conn = pymysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            port=int(os.getenv("DB_PORT")),
            cursorclass=DictCursor
        )
        print("MySQL 연결 성공")
    except Exception as e:
        print("MySQL 연결 실패:", e)
        return

    cursor = conn.cursor()

    # 마지막 동기화 시각 읽기
    try:
        with open("../data/last_sync.txt", "r") as f:
            last_sync = f.read() or "1970-01-01 00:00:00"
    except FileNotFoundError:
        last_sync = "1970-01-01 00:00:00"

    # MySQL 데이터 가져오기 (증분 반영)
    query = """
        SELECT i.id as ingredientId,
            i.name as ingredientName,
            i.updatedAt,
            c.id as categoryId,
            c.name as categoryName
        FROM ingredients i
        JOIN categories c ON i.categoryId = c.id
        WHERE i.updatedAt > %s
    """
    cursor.execute(query, (last_sync,))
    rows = cursor.fetchall()

    print(f"MySQL에서 가져온 데이터 수: {len(rows)}")

    # Elasticsearch bulk 색인
    actions = [
        {
            "_index": INDEX_NAME,
            "_id": row["ingredientId"],
            "_source": {
                "ingredientId": row["ingredientId"],
                "ingredientName": row["ingredientName"],
                "ingredientName_nospace": row["ingredientName"].replace(" ", ""),
                "categoryName": row["categoryName"],
                "categoryId": row["categoryId"],
                "updatedAt": row["updatedAt"]
            }
        }
        for row in rows
    ]

    if actions:
        try:
            bulk(es, actions)
            es.indices.refresh(index=INDEX_NAME)
            print(f"Elasticsearch 색인 완료: {len(actions)}건")
        except Exception as e:
            print("Elasticsearch 색인 실패:", e)
    else:
        print("새로 색인할 데이터 없음")

    # 마지막 동기화 시각 기록
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with open("../data/last_sync.txt", "w") as f:
        f.write(now)

    conn.close()
    print("MySQL 연결 종료")

    # 동기화 후 문서 수
    count_after = es.count(index=INDEX_NAME)['count']
    print(f"동기화 후 문서 수: {count_after}")
    print(f"이번 동기화로 추가/업데이트된 문서 수: {count_after - count_before}")


if __name__ == "__main__":
    sync_to_es()