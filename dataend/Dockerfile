FROM python:3.11-slim

WORKDIR /app

COPY app/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app .

COPY product_name_categories.csv /data/product_name_categories.csv

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
