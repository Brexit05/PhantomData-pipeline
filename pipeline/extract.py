import requests
import pandas as pd
from pipeline.config import URL , PARAM, PG_DB , LAT, LONG, DATE
import logging

logging.basicConfig(level = logging.INFO,
                    filename='/opt/airflow/logs/extract.log',
                    format = "%(asctime)s - %(levelname)s - %(message)s",
                    handlers = [logging.FileHandler('extract.log'), logging.StreamHandler()])

def extract_police_api():
    """"This function extracts data fromthe police API 
        and converts it to a Dataframe and stores it in locally in CSV format."""
    try:
        logging.info(f"Extracting data from police API with latitude:{LAT},longitude: {LONG} on {DATE}.")
        response = requests.get(URL, params={"lat": LAT, "lng": LONG, "date": DATE} )
        response.raise_for_status()
        df = response.json()
        data = pd.json_normalize(df)
        name = PG_DB.lower().strip() + '.csv'
        data.to_csv(name, index = False)
        logging.info(f"Data extracted and saved to {name}.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from police API: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

extract_police_api()