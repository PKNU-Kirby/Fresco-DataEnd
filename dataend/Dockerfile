FROM python:3.12-slim

WORKDIR /dataend

COPY requirements.txt /dataend/

RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY app/services ./app/services
COPY scripts ./scripts

ENV PYTHONPATH=/dataend/app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

