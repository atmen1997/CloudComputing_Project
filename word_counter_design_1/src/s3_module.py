from src.sqs_module import get_messages_from_sqs
import boto3
import json
import os

# Function to get object location from SQS messages
def get_object_location(queue_name):
    messages = get_messages_from_sqs(queue_name)
    if not messages:
        print("No messages received.")
        return None
    for message in messages:
        # Parse the message body
        body = json.loads(message['Body'])
        print("Received message:", body)

        # Extract S3 bucket and object key
        bucket_name = body["Records"][0]["s3"]["bucket"]["name"]
        object_key = body["Records"][0]["s3"]["object"]["key"]
        print(f"Get file location from queue success: s3://{bucket_name}/{object_key}")
        
        # Delete the processed message
        message.delete()
        print("Message deleted from the queue.")
        return bucket_name, object_key  

# Function to get and read the object
def get_object_content(bucket_name, object_key, start_byte=None, end_byte=None):
    # Initialize S3 client
    s3 = boto3.client("s3")
    print(f"Processing file: s3://{bucket_name}/{object_key}")

    # Prepare the Range header if byte range is specified
    if start_byte is not None and end_byte is not None:
        range_header = f"bytes={start_byte}-{end_byte - 1}"
        response = s3.get_object(Bucket=bucket_name, Key=object_key, Range=range_header)
    else:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)

    # Read and decode the response content
    file_content = response["Body"].read().decode("utf-8")
    return file_content

# Function to upload a file to S3
def upload_file(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"{file_name} successfully uploaded to bucket: {bucket} as object: {object_name}")
        return
    except Exception as e:
        print(f"Error uploading file {file_name} to bucket {bucket}: {e}")
    

# Function to delete an S3 object
def delete_s3_object(bucket_name, object_key):
    # Initialize the S3 client
    s3 = boto3.client("s3")

    try:
        # Delete the object
        response = s3.delete_object(Bucket=bucket_name, Key=object_key)
        print(f"Deleted object: s3://{bucket_name}/{object_key}")
        return response
    except Exception as e:
        print(f"Error deleting object: {e}")