# Police Incident ETL Pipeline 🚓

This project extracts data from a police crime API, processes it using Apache Airflow, and stores it into Postgres and cloud storage. It follows a medallion architecture (bronze → silver → gold).

## 🔧 Tools
- Apache Airflow
- Docker Compose
- Python (3.12)
- PostgreSQL
- Cloud storage (S3-compatible)
- Pandas, Requests

## ⚙️ Pipeline Overview
1. **Extract**: Data is pulled from a public API.
2. **Store Raw**: Raw data is saved in local/bronze storage.
3. **Silver Layer**: Cleaned and structured data in PostgreSQL.
4. **Gold Layer**: Aggregated insights and business-ready tables.
5. **Cloud Upload**: Data pushed to S3 bucket.

## 📂 Project Structure
dags/ 
pipeline/
├── extract.py
├── s3Bucket.py
├── config.py
├── storeRaw.py
├── transformData.py
└── goldLayer.py
docker-compose.yaml # Airflow + Postgres setup
Dockerfile 
requirements.txt 

## 🚀 Running the Project

docker compose up --build
Then visit: http://localhost:8080
Use Airflow UI to trigger police_etl_dag.

🧪 Coming Soon
Pytest-based unit testing

Cloud deployment to AWS