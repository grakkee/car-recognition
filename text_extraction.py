#Grace Meredith
#CS 442 HW1 Part 2: Text Detection

#Algorithm: Recieve messages from sqs queue to get filenames w/ cars, use rekognition to find text (if any) in those images, Write the names of files along with their text to a txt file.

import os
import sys
import boto3
from pathlib import Path

aws_access_key_id = '<your acces key>'
aws_secret_access_key = '<your secret key>'
region = '<your region>'

def get_sqs_messages():
	sqs = boto3.resource('sqs', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
	queue = sqs.get_queue_by_name(QueueName='Car-Detection-Queue')
	max_queue_messages = 10
	message_bodies = []
	while True:
		messages_to_delete = []
		for message in queue.receive_messages(
			MaxNumberOfMessages=max_queue_messages):
			# process message body
			message_bodies.append(message.body)
			# add message to delete
			messages_to_delete.append({
				'Id': message.message_id,
				'ReceiptHandle': message.receipt_handle
				})
		# if you don't receive any notifications the
		# messages_to_delete list will be empty
		if len(messages_to_delete) == 0:
			break
			# delete messages to remove them from SQS queue
			# handle any errors
		else:
			delete_response = queue.delete_messages(
				Entries=messages_to_delete)
	find_text(message_bodies)

def find_text(file_names):
	rek_client = boto3.client('rekognition',region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
	labels = []


	for file_name in file_names:
		wrtie_current_file(file_name)

		with open(file_name, 'rb') as im:
			# Read image bytes
			im_bytes = im.read()
			# Upload image to AWS 
			response = rek_client.detect_text(Image={'Bytes': im_bytes})
			textDetections = response['TextDetections']

			for text in textDetections:
				write_text(text['DetectedText'])

def wrtie_current_file(file_name):
	file1 = open("output.txt", "a")  # append mode
	file1.write("\n" + file_name + "\n")
	file1.close()

def write_text(text):
	file1 = open("output.txt", "a")  # append mode
	file1.write(text + "\n")
	file1.close()

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


def main():
	client = boto3.client("s3", region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
	file_names = get_file_folders(client, "cs442-unr")
	download_files(client,"cs442-unr","s3-photos", file_names)

	get_sqs_messages()

if __name__ == "__main__":
	main()