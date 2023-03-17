#Grace Meredith
#CS 442 HW1 Part 1: Car Recognition

#algorithm: get file names from s3 bucket, download files, using rekognition to detect cars, send filenames with cars to sqs queue.


import boto3
import os
import sys
from pathlib import Path

aws_access_key_id = '<your acces key>'
aws_secret_access_key = '<your secret key>'
region = '<your region>'


def get_file_folders(s3_client, bucket_name, prefix=""):
    file_names = []

    default_kwargs = {
        "Bucket": bucket_name,
        "Prefix": prefix
    }
    next_token = ""

    while next_token is not None:
        updated_kwargs = default_kwargs.copy()
        if next_token != "":
            updated_kwargs["ContinuationToken"] = next_token

        response = s3_client.list_objects_v2(**default_kwargs)
        contents = response.get("Contents")

        for result in contents:
            key = result.get("Key")
            file_names.append(key)

        next_token = response.get("NextContinuationToken")

    return file_names


def download_files(s3_client, bucket_name, local_path, file_names):
    local_path = Path(local_path)

    for file_name in file_names:
        file_path = Path.joinpath(local_path, file_name)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        s3_client.download_file(bucket_name, file_name, str(file_path))


def find_cars(file_name):
    rek_client = boto3.client('rekognition', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    labels = []

    with open(file_name, 'rb') as im:
        # Read image bytes
        im_bytes = im.read()
        # Upload image to AWS 
        response = rek_client.detect_labels(Image={'Bytes': im_bytes}, MinConfidence=90)

        for label in response['Labels']:
            if "Car" in label['Name']:
                send_to_queue(file_name)

def send_to_queue(file_name):
    sqs = boto3.resource('sqs', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    queue = sqs.get_queue_by_name(QueueName='Car-Detection-Queue')
    # Create a new message
    response = queue.send_message(MessageBody=file_name)

def main():
    client = boto3.client("s3", region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    file_names = get_file_folders(client, "cs442-unr")
    download_files(client,"cs442-unr","s3-photos",file_names)

    for file in file_names:
        path = os.path.join("s3-photos", file)
        find_cars(path)

if __name__ == "__main__":
    main()

