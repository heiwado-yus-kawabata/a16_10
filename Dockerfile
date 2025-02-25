FROM tiangolo/uvicorn-gunicorn-fastapi:python3.11

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY ./app /app

ENV PYTHONPATH /app/routers:/app/config
ENV TIMEOUT 3000