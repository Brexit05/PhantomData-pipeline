import boto3
import botocore
from pipeline.config import AWS_ID, AWS_SECRET, AWS_REGION
import logging

logging.basicConfig(level = logging.INFO,
                    filename='/opt/airflow/logs/s3Bucket.log',
                    format= "%(asctime)s - %(levelname)s - %(message)s",
                    handlers = [logging.FileHandler('s3Bucket.log'), logging.StreamHandler()])

def s3_upload(file_to_upload,bucket,name_of_file_uploaded):
    """This function uploads a file to an S3 bucket. it takes the file to upload,
      the bucket name, and the name of the file in S3."""
    try:

        logging.info(f"Uploading {file_to_upload} to bucket {bucket} as {name_of_file_uploaded}.")
        s3 = boto3.client('s3',
                        aws_access_key_id = AWS_ID,
                        aws_secret_access_key = AWS_SECRET,
                        region_name = AWS_REGION)
        s3.upload_file(file_to_upload,bucket,name_of_file_uploaded)
        logging.info(f"File {name_of_file_uploaded} uploaded successfully to {bucket}.")
    except botocore.exceptions.ClientError as e:
        logging.error(f"Upload failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise


# I didnt make use of this function in my pipeline. I wrote it incase you wanted to download the CSV file from cloud.
def s3_download(file_to_download, bucket,file_new_name):
    """This function downloads a file from an S3 bucket. It takes the file to download,
      the bucket name, and the new name for the downloaded file."""
    try:
        logging.info(f"Downloading {file_to_download} from bucket {bucket}.")
        s3 = boto3.client('s3',
                        aws_access_key_id = AWS_ID,
                        aws_secret_access_key = AWS_SECRET,
                        region_name = AWS_REGION)
        s3.download_file(bucket, file_to_download, file_new_name)
        logging.info(f"File {file_to_download} downloaded successfully as {file_new_name}.")
    except botocore.exceptions.ClientError as e:
        logging.error(f"Download failed: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise