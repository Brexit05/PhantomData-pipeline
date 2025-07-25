from sqlalchemy import create_engine, text
import logging
from pipeline.config import AWS_BUCKET, PG_URL , TABLE_NAME
from sqlalchemy.exc import (
    SQLAlchemyError,
    OperationalError,
    DatabaseError
)
import pandas as pd
import os
from pipeline.s3Bucket import s3_upload

logging.basicConfig(level = logging.INFO,
                    filename='/opt/airflow/logs/transformData.log',
                    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[
        logging.FileHandler("police_silver.log"),
        logging.StreamHandler()
    ])


def creating_silver_table():
    """This function creates the silver table in PostgreSQL database."""
    try:
        table_silver = TABLE_NAME.lower().strip() + '_silver_data'
        logging.info(f"Creating {table_silver} table if it does not exist.")
        engine = create_engine(PG_URL)
        query = text(f"""DROP TABLE IF EXISTS {table_silver}; 
                     CREATE TABLE IF NOT EXISTS {table_silver} (
                    id INT PRIMARY KEY,
                    persistent_id VARCHAR(100),
                    context VARCHAR(100),
                    category VARCHAR(100),
                    month DATE,
                    location_street_id INT,
                    location_street_name VARCHAR(100),
                    location_type VARCHAR(100),
                    location_subtype VARCHAR(100),
                    location_lat DECIMAL(10,6),
                    location_lng DECIMAL(10,6),
                    outcome_status VARCHAR(100),
                    outcome_status_cat VARCHAR(100),
                    outcome_status_date DATE,
                    dwh_date_logged TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    """)
        with engine.begin() as conn:
            conn.execute(query)
        logging.info("police_silver_data successfuly created")
        engine.dispose()
    except OperationalError as e:
        logging.error(f"Database connection error: {e}")
        
    except DatabaseError as e:
        logging.error(f"Database error occurred: {e}")
        
    except SQLAlchemyError as e:
        logging.error(f"SQLAlchemy error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in creating {table_silver}: {e}")

def filling_silver_layer():
    """This function fills the silver table with transformed data from the bronze layer."""
    try:
        table_raw = TABLE_NAME.lower().strip() + '_raw_data'
        table_silver = TABLE_NAME.lower().strip() + '_silver_data'
        logging.info(f"Loading into {table_silver} table")
        engine = create_engine(PG_URL)
        query = text(f"""TRUNCATE TABLE {table_silver}; 
                        INSERT INTO {table_silver} (id,
                                                        persistent_id,
                                                        context,
                                                        category,
                                                        month,
                                                        location_street_id,
                                                        location_street_name,
                                                        location_type,
                                                        location_subtype,
                                                        location_lat,
                                                        location_lng,
                                                        outcome_status,
                                                        outcome_status_cat,
                                                        outcome_status_date)
                        SELECT id,
                                TRIM(COALESCE(persistent_id::TEXT, '')) AS persistent_id,
                                TRIM(COALESCE(context::TEXT, '')) AS context,
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(category::TEXT, ''))) LIKE '%theft%' OR LOWER(TRIM(COALESCE(category::TEXT, ''))) IN ('burglary','shoplifting','vehicle-crime') THEN 'property crime'
                                    WHEN LOWER(TRIM(COALESCE(category::TEXT, ''))) IN ('violent-crime','robbery','possession-of-weapons') THEN 'violent crime'
                                    WHEN LOWER(TRIM(COALESCE(category::TEXT, ''))) = 'criminal-damage-arson' THEN 'property damage'
                                    ELSE 'public order crime'
                                END as category,
                                CAST(CONCAT(month,'-01') AS DATE) AS month,
                                "location.street.id" AS location_street_id,
                                CASE
                                    WHEN LOWER(TRIM(COALESCE("location.street.name"::TEXT, ''))) = 'on or near' THEN 'unspecified location'  
                                    WHEN LOWER(TRIM(COALESCE("location.street.name"::TEXT, ''))) LIKE 'on or near%' THEN REPLACE("location.street.name", 'On or near' , 'In the vicinity of')
                                    ELSE COALESCE("location.street.name", '')
                                END AS location_street_name,
                                CASE
                                    WHEN LOWER(TRIM(COALESCE(location_type::TEXT, ''))) = 'force' THEN 'Territorial Police Force'
                                    WHEN LOWER(TRIM(COALESCE(location_type::TEXT, ''))) = 'btp' THEN 'British Transport Police'
                                    ELSE 'Unknown'
                                END AS location_type,
                                CASE
                                    WHEN TRIM(COALESCE(location_subtype::TEXT, '')) = '' THEN 'NOT SPECIFIED'
                                    ELSE UPPER(TRIM(COALESCE(location_subtype::TEXT, '')))
                                END AS location_subtype,
                                CASE
                                    WHEN TRIM(COALESCE("location.latitude"::TEXT, '')) = '' THEN NULL
                                    ELSE CAST(TRIM("location.latitude"::TEXT) AS DECIMAL(10,6))
                                END AS location_lat,
                                CASE
                                    WHEN TRIM(COALESCE("location.longitude"::TEXT, '')) = '' THEN NULL
                                    ELSE CAST(TRIM("location.longitude"::TEXT) AS DECIMAL(10,6))
                                END AS location_lng,
                                CASE
                                    WHEN TRIM(COALESCE(outcome_status::TEXT, '')) = '' THEN '-'
                                    ELSE TRIM(COALESCE(outcome_status::TEXT, ''))
                                END AS outcome_status,
                                TRIM(COALESCE("outcome_status.category"::TEXT, '')) AS outcome_status_cat,
                                CASE
                                    WHEN TRIM(COALESCE("outcome_status.date"::TEXT, '')) = '' THEN NULL
                                    ELSE CAST(CONCAT("outcome_status.date", '-01') AS DATE)
                                END AS outcome_status_date
                        FROM {table_raw};""")
        with engine.begin() as conn:
             conn.execute(query)
        logging.info(f"{table_silver} table loaded.")
    except OperationalError as e:
        logging.error(f"Database connection error: {e}")
        
    except DatabaseError as e:
        logging.error(f"Database error occurred: {e}")
        
    except SQLAlchemyError as e:
        logging.error(f"SQLAlchemy error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in creating table: {e}")


def silver_to_cloud():
    """This function uploads the silver table to S3 bucket."""
    try:
        table_silver = TABLE_NAME.lower().strip() + '_silver_data'
        logging.info("Beginning silver to cloud process.")
        engine = create_engine(PG_URL)
        logging.info("Reading from Silver table.")
        silver_table = pd.read_sql(text(f"""SELECT * FROM {table_silver};"""), engine)
        csv = table_silver + '.csv'
        silver_table.to_csv(csv, index = False)
        if os.path.exists(csv):
            s3_upload(csv, AWS_BUCKET, f"silver/{csv}")
            logging.info("Silver process completed")
        else:
            logging.warning(f"{csv} file not found, skipping process.")
        os.remove(csv)
        engine.dispose()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

creating_silver_table()
filling_silver_layer()
silver_to_cloud()
