import pandas as pd
from pipeline.s3Bucket import s3_upload
from pipeline.config import PG_URL, PG_DB , TABLE_NAME, AWS_BUCKET, BASE_PATH
from sqlalchemy import create_engine
import logging
from sqlalchemy.exc import (
    SQLAlchemyError,
    OperationalError,
    DatabaseError
)
import pandas as pd
import os

logging.basicConfig(
    level=logging.INFO,
    filename='/opt/airflow/logs/storeRaw.log',
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[
        logging.FileHandler("storeRaw.log"),
        logging.StreamHandler()])


def store_raw_data():
    """"This function collects the csv file stored locally, and saves it to PostgreSQL 
    and S3 bucket, then deletes the local file."""
    logging.info(f"Storing data to PostgreSQL, {PG_DB} and S3 Bucket, {AWS_BUCKET}.")
    try:
        name = PG_DB.lower().strip() + '.csv'
        name_path = os.path.join( BASE_PATH , name)
        logging.info(f"Checking if the data file {name} exists.")
        if not os.path.exists(name):
            raise FileNotFoundError(f"File {name} not found.")
        logging.info(f"File {name} found, proceeding to upload.")
        data = pd.read_csv(name_path)
        logging.info(f"Executing PostgreSQL process.")
        engine = create_engine(PG_URL)
        table_raw = TABLE_NAME.lower().strip() + '_raw_data'
        data.to_sql(table_raw, engine , index=False, if_exists = "replace")
        logging.info(f"{table_raw} successfully loaded into PostgreSQL.")
        engine.dispose()
        logging.info(f"Loading data into {AWS_BUCKET} bucket.")
        filename = 'raw/' + name
        s3_upload(name, AWS_BUCKET, filename)
        logging.info(f"Data successfully uploaded to S3 Bucket , {AWS_BUCKET}")
        if os.path.exists(name):
            os.remove(name)
            logging.info(f"Local file {name} deleted successfully.")
        else:
            logging.warning(f"Local file {name} not found for deletion.")
    except OperationalError as e:
        logging.error(f"Database connection error: {e}")
        
    except DatabaseError as e:
        logging.error(f"Database error occurred: {e}")
        
    except SQLAlchemyError as e:
        logging.error(f"SQLAlchemy error: {e}")   
    except Exception as e:
        logging.error(f"Unexpected error during bronze layer execution: {e}")
         

store_raw_data()

