FROM python:3.12.12-slim-bookworm
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py .
USER 1000:1000
ENV PYTHONUNBUFFERED=1
CMD ["python", "producer.py"]
