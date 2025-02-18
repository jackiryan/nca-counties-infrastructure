FROM python:3.12-slim

RUN pip install fastapi httpx psycopg2-binary>=2.9.9 pydantic sqlalchemy uvicorn

WORKDIR /app
COPY api/climate_vars.py .

CMD ["python", "climate_vars.py"]