from dotenv import load_dotenv
import os


load_dotenv()

URL = os.getenv('url','https://data.police.uk/api/crimes-street/all-crime')
LAT = os.getenv('lat')
DATE = os.getenv('date')
LONG = os.getenv('lng')

PARAM = {
    'lat': os.getenv('lat'),
    'lng': os.getenv('lng'),
    'date': os.getenv('date')}

PG_HOST = os.getenv('pg_host')
PG_PORT = os.getenv('pg_port')
PG_DB = os.getenv('pg_name')
PG_USER = os.getenv('pg_user')
PG_PASSWORD = os.getenv('pg_password')

PG_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

AWS_ID = os.getenv('access_key_id')
AWS_SECRET = os.getenv('secret_access_key')
AWS_REGION = os.getenv('aws_region')
AWS_BUCKET = os.getenv('aws_bucket')

TABLE_NAME = os.getenv('table_name')

BASE_PATH = os.getenv('base_path',"C:\\Users\\wwwar\\OneDrive\\Desktop\\POLICE-ETL")