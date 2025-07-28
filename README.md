# DataEnd
데이터 관련 리포지토리입니다.

## 1. crawling.py
유통상품 표준DB 상품분류 크롤러
[유통상품 표준DB](https://www.allproductkorea.or.kr/products/database/category)의 상품 분류 정보를 자동으로 크롤링하여 CSV 파일로 저장하는 Python 스크립트입니다. 
대분류부터 세분류, 예시 상품명까지 계층적으로 수집하여 데이터 분석 또는 머신러닝 학습에 활용할 수 있도록 가공합니다.

### Features
- 유통상품 표준DB 사이트에서 상품 분류 트리 구조(대분류 → 중분류 → 소분류 → 세분류)를 자동으로 탐색
- 각 세분류에 해당하는 예시 상품명 수집
- 계층 구조 + 예시 데이터를 CSV(`product_categories.csv`)로 저장
- 예시 항목이 여러 개인 경우 개별 레코드로 분리 저장

## 2. FastAPI Product Search API
제품명 검색 및 매핑용 API 입니다.

### Features
- ElasticSearch를 활용한 긴 재료명을 리스트의 상품명으로 매핑
- 매핑된 상품명에 해당하는 카테고리 매핑

### Structure
```bash
dataend/
├── .env
├── docker-compose.yml
├── Dockerfile
├── product_name_categories.csv
├── app/
│   ├── main.py
│   ├── bulk_index.py
|   └── requirements.txt
```

### API Endpoints

| 메서드 | 경로                           | 설명                   |
|--------|--------------------------------|------------------------|
| GET    | `/search`                      | 문자열에 대해 유사도가 높은 상품명 검색         |
