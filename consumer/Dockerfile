FROM python:3.11-slim

WORKDIR /app

COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 공유 모듈 복사
COPY src/storage/pgvector.py ./storage/pgvector.py
COPY src/crawler/base.py ./crawler/base.py
RUN touch storage/__init__.py crawler/__init__.py

COPY consumer/consumer.py .

CMD ["python", "-u", "consumer.py"]
