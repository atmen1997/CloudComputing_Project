import json
import boto3
import time

# Function to send chunks to SQS
def send_message_to_sqs(queue_name, data, custom_hash_id=None):
    sqs = boto3.client("sqs", region_name="us-east-1")  # Specify your region
    try:
        # Get the queue URL using the queue name
        response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']

        # Prepare the message attributes for FIFO queue if necessary
        if queue_name.endswith(".fifo"):
            if custom_hash_id:
                response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(data), MessageGroupId="default", MessageDeduplicationId=custom_hash_id)
            else:
                response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(data), MessageGroupId="default")
        else:
            response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(data))

        print(f"Sent data to SQS: {data}, MessageId: {response['MessageId']}")

    except Exception as e:
        print(f"Failed to send data to SQS: {data}. Error: {e}")

# Function to receive messages from SQS
def get_messages_from_sqs(queue_name, max_messages=1, wait_time_seconds=3):
    sqs = boto3.client("sqs", region_name="us-east-1")  # Specify your region
    messages = []
    try:
        # Get the queue URL using the queue name
        response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']

        # Receive a batch of messages
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time_seconds  # Long polling to minimize empty responses
        )

        # Check if there are messages in the response
        if 'Messages' in response:
            messages = response['Messages']
            print(f"Fetched {len(messages)} messages from queue ({queue_name}).")
        else:
            print(f"No messages available in the queue ({queue_name}).")

    except Exception as e:
        print(f"Error retrieving messages from SQS: {e}")

    return messages

# Function to delete a message from SQS
def delete_message_from_sqs(queue_name, receipt_handle):
    sqs_client = boto3.client("sqs", region_name="us-east-1")  # Specify your region
    try:
        # Get the queue URL using the queue name
        response = sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']

        # Delete the message
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print(f"Deleted message from {queue_name}")
    except Exception as e:
        print(f"Error deleting message from SQS {queue_name}: {e}")

# Function to purge all SQS queues
def purge_all_queues():
    sqs = boto3.client("sqs")

    try:
        # List all SQS queues
        response = sqs.list_queues()
        if "QueueUrls" not in response:
            print("No SQS queues found.")
            return

        queue_urls = response["QueueUrls"]

        for queue_url in queue_urls:
            print(f"Purging queue: {queue_url}")
            try:
                sqs.purge_queue(QueueUrl=queue_url)
                print(f"Successfully purged queue: {queue_url}")
            except sqs.exceptions.PurgeQueueInProgress:
                print(f"Queue is already being purged: {queue_url}")
            except Exception as e:
                print(f"Error purging queue {queue_url}: {e}")
    except Exception as e:
        print(f"Error listing queues: {e}")
# purge_all_queues()
# Function to monitor SQS queue activity
def monitor_queues(queue_name, ApproximateNumberOfMessages=1, sleep_time=3, timeout=60):
    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    
    start_time = time.time()

    while True:
        queue.load()  # Ensures attributes like ApproximateNumberOfMessages are up to date
        # Check the number of messages in the queue
        process_messages = int(queue.attributes.get("ApproximateNumberOfMessages", 0))

        print(f"Available tasks in queue ({queue_name}): {process_messages}")

        # Exit if condition is met
        if process_messages >= ApproximateNumberOfMessages:
            print("Ready to continue process")
            break

        # Check if the timeout is exceeded
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout:
            print(f"Timeout of {timeout} seconds reached while waiting for queue activity.")
            break
            # raise TimeoutError(f"Queue {queue_name} did not meet the condition within {timeout} seconds.")

        # Wait before polling again
        time.sleep(sleep_time)
    return process_messages
    
