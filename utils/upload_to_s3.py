import boto3
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
import os
from loguru import logger
from dotenv import load_dotenv


def _get_client():
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_session_token = os.environ.get("AWS_SESSION_TOKEN")
    if aws_access_key_id and aws_secret_access_key:
        return boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )
    else:
        raise Exception("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not set")

def upload_file(file_path:Path, bucket:str, folder_path_in_bucket:str):
    # Upload the file
    if not folder_path_in_bucket.endswith("/"):
        folder_path_in_bucket += "/"
    if folder_path_in_bucket.startswith("/"):
        folder_path_in_bucket = folder_path_in_bucket[1:]
    if not file_path.is_file():
        raise Exception(f"{file_path} is not a file")

    s3_client = _get_client()
    object_name = folder_path_in_bucket + file_path.name
    try:
        logger.info(f"Uploading {file_path} to {bucket}/{object_name}")
        response = s3_client.upload_file(str(file_path), bucket, object_name)
    except ClientError as e:
        logger.error(e)
        return False
    return True


if __name__ == "__main__":
    load_dotenv()

    folder_path = Path(r"UNSW-NB15/Packet-Fields")
    bucket = "s3-9212-lz-d-001"
    folder_path_in_bucket = "TEST_WANG/Packet_Fields/"
    for file_path in folder_path.iterdir():
        if file_path.is_file():
            upload_file(file_path, bucket, folder_path_in_bucket)