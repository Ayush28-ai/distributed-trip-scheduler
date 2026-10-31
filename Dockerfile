# One Python image for api, node1-3 and ui; docker-compose picks the command.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY ui ./ui
COPY migrations ./migrations

EXPOSE 8000 8501 9100
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
