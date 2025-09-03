from elasticsearch import Elasticsearch
from datetime import datetime, timezone
from elasticsearch.helpers import bulk
from pymysql.cursors import DictCursor

import os
import pymysql
import json

INDEX_NAME = "products"

es = Elasticsearch("http://elasticsearch:9200").options(request_timeout=60)

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
    if not es.ping():
        print("[ERROR] Elasticsearch 연결 실패")
        return

    create_index_if_not_exists()

    count_before = es.count(index=INDEX_NAME)["count"]
    print(f"[INFO] 동기화 전 문서 수: {count_before}")

    # MySQL 연결
    try:
        conn = pymysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            port=int(os.getenv("DB_PORT")),
            cursorclass=DictCursor
        )
        print("[INFO] MySQL 연결 성공")
    except Exception as e:
        print("[ERROR] MySQL 연결 실패:", e)
        return

    cursor = conn.cursor()

    # MySQL 전체 데이터 조회
    query = """
        SELECT i.id as ingredientId,
               i.name as ingredientName,
               i.updatedAt,
               c.id as categoryId,
               c.name as categoryName
        FROM ingredients i
        JOIN categories c ON i.categoryId = c.id
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    print(f"[INFO] MySQL에서 가져온 전체 데이터 수: {len(rows)}")

    # ES에 존재하는 문서 조회
    resp = es.search(index=INDEX_NAME, size=2000, body={"query": {"match_all": {}}})
    existing_docs = {doc["_id"]: doc["_source"] for doc in resp["hits"]["hits"]}

    mysql_ids = set()
    actions = []

    # 신규/변경/삭제 카운트
    new_count = 0
    update_count = 0

    # 1. 신규/변경 문서 처리
    for row in rows:
        doc_id = str(row["ingredientId"])
        mysql_ids.add(doc_id)

        source = {
            "ingredientId": row["ingredientId"],
            "ingredientName": row["ingredientName"],
            "ingredientName_nospace": row["ingredientName"].replace(" ", ""),
            "categoryName": row["categoryName"],
            "categoryId": row["categoryId"],
            "updatedAt": row["updatedAt"].strftime("%Y-%m-%dT%H:%M:%S") if hasattr(row["updatedAt"], "strftime") else row["updatedAt"]
        }

        if doc_id not in existing_docs:
            # 신규 문서
            actions.append({"_index": INDEX_NAME, "_id": doc_id, "_source": source})
            new_count += 1
            print(f"[NEW] ID: {doc_id}, 내용: {source}")
        else:
            # 변경 여부 확인 (변경된 컬럼만 표시)
            old = existing_docs[doc_id]
            differences = {}
            for key in source:
                if source[key] != old.get(key):
                    differences[key] = {"이전": old.get(key), "이후": source[key]}

            if differences:
                actions.append({"_index": INDEX_NAME, "_id": doc_id, "_source": source})
                update_count += 1
                print(f"[UPDATE] ID: {doc_id}")
                for k, v in differences.items():
                    print(f" - {k}: [이전] {v['이전']} → [이후] {v['이후']}")

    # 2. ES에만 존재하는 문서 삭제
    to_delete = set(existing_docs.keys()) - mysql_ids
    delete_count = len(to_delete)
    for doc_id in to_delete:
        doc = existing_docs[doc_id]
        print(f"[DELETE] ID: {doc_id}, 내용: {doc}")
        es.options(ignore_status=[404]).delete(index=INDEX_NAME, id=doc_id)

    # 3. Bulk 색인
    if actions:
        success, _ = bulk(es, actions)
    else:
        success = 0

    # 4. 요약 출력
    print(f"[INFO] 색인/갱신 완료: {success}건 (신규: {new_count}, 변경: {update_count}, 삭제: {delete_count})")

    es.indices.refresh(index=INDEX_NAME)
    conn.close()
    print("[INFO] MySQL 연결 종료")

    count_after = es.count(index=INDEX_NAME)["count"]
    print(f"[INFO] 동기화 후 문서 수: {count_after}")

if __name__ == "__main__":
    sync_to_es()