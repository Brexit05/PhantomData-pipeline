from sqlalchemy import create_engine,text
import logging
import pandas as pd
from pipeline.config import  TABLE_NAME, PG_URL, AWS_BUCKET
from sqlalchemy.exc import (
    SQLAlchemyError,
    OperationalError,
    DatabaseError
)
import os
from pipeline.s3Bucket import s3_upload

logging.basicConfig(level = logging.INFO,
                    filename='/opt/airflow/logs/goldlayer.log',
                    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[logging.FileHandler("goldlayer.log"), logging.StreamHandler()])


table_gold = TABLE_NAME.lower().strip() + '_gold_crime_summary'

def creating_gold_table():
    try:
        logging.info(f"Creating {table_gold} table if it does not exist.")
        engine = create_engine(PG_URL)
        query = text(f""" CREATE TABLE IF NOT EXISTS {table_gold} (category VARCHAR(50),
                                                                                month VARCHAR(50),
                                                                                total_crimes INT )""")
        with engine.begin() as conn:
                conn.execute(query)
        logging.info("Table created.")
    except OperationalError as e:
        logging.error(f"Database connection error: {e}")
        raise
    except DatabaseError as e:
        logging.error(f"Database error occurred: {e}")
        raise
    except SQLAlchemyError as e:
        logging.error(f"SQLAlchemy error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in creating gold table: {e}")

def loading_gold_table():
    try:
        table_silver = TABLE_NAME.lower().strip() + '_silver_data'
        logging.info(f"Loading into {table_gold}.")
        engine = create_engine(PG_URL)
        query = text(f"""TRUNCATE TABLE {table_gold};
                        INSERT INTO {table_gold}
                        SELECT category,
                                TO_CHAR(month, 'YYYY-MM') AS month,
                                COUNT(id) AS total_crimes
                        FROM {table_silver}
                        GROUP BY category,TO_CHAR(month, 'YYYY-MM')
                        ORDER BY TO_CHAR(month, 'YYYY-MM') DESC """)
        with engine.begin() as conn:
            conn.execute(query)
        logging.info(f"{table_gold} loaded.")
    except OperationalError as e:
        logging.error(f"Database connection error: {e}")
        
    except DatabaseError as e:
        logging.error(f"Database error occurred: {e}")
        
    except SQLAlchemyError as e:
        logging.error(f"SQLAlchemy error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in creating gold table: {e}")


def gold_to_s3():
        try:
            logging.info("Beginning gold to cloud process.")
            engine = create_engine(PG_URL)
            query = text(f""" SELECT * 
                             FROM {table_gold}; """)
            police_gold_data = pd.read_sql(query,engine)
            police_gold_data.to_csv(f"{table_gold}.csv", index = False)
            if os.path.exists(f"{table_gold}.csv"):
                s3_upload(f"{table_gold}.csv", AWS_BUCKET, f"gold/{table_gold}.csv")
                logging.info(f"Successfully loaded {table_gold} to cloud.")
            else:
                logging.warning(f"{table_gold} file not found, Skipping uploading to S3 bucket police-data-pipeline")
            os.remove(f"{table_gold}.csv")
        except Exception as e:
            logging.error(f"Issue converting to CSV : {e}")

creating_gold_table()
loading_gold_table()
gold_to_s3()