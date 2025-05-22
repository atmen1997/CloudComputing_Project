from src.sqs_module import send_message_to_sqs, get_messages_from_sqs, delete_message_from_sqs
from src.text_processing_module import word_count, generate_sha256_hash
from src.s3_module import get_object_content, upload_file
from src.ec2_module import get_ec2_info
from src.sqs_module import monitor_queues
import time
import json
import os
import boto3

# Initialize a global SQS client
sqs = boto3.client('sqs')

def run_ec2(
    ec2_name,
    max_line_per_chunk,
    queue_size,
    wait_time,
    **queue_name
):
    ec2_count = len(get_ec2_info())
    process_queue = queue_name["process_queue"]  
    result_queue  = queue_name["result_queue"]  

    # Monitor the process queue
    monitor_queues(process_queue, 1, 5, 180) 
    # Tells EC2 when to start processing if queue has some messages

    # Measure start time
    start_time = time.time()

    bucket_name = None
    object_key = None
    chunk_file_name = None
    chunk_word_count = 0
    chunk_count = 0
    process_time = 0
    hash_ids = []

    loop_start_time = time.time()
    timeout = 360
    while True:
        current_time = time.time()
        loop_elapsed_time = current_time - loop_start_time

        # Break the loop if the timeout is reached
        if loop_elapsed_time > timeout:
            print(f"Timeout of {timeout} seconds reached.")
            break

        messages = get_messages_from_sqs(process_queue, queue_size, wait_time)

        # If no messages and we have already processed chunks, break
        if not messages and monitor_queues(process_queue, 1, 1, 20) == 0:
                print("No more messages in queue after retries. Exiting.")
                break
        
        elif messages:
            for message in messages:
                try:
                    # Measure start time for each message
                    start_time_msg = time.time()

                    body = json.loads(message['Body'])
                    print(f"Received body: {body}")

                    # Get chunk details
                    bucket_name = body["bucket_name"]
                    object_key = body["object_key"]
                    chunk_file_name = body["chunk_file_name"]
                    start_line = body["start_line"]
                    end_line = body["end_line"]
                    start_byte = body["start_byte"]
                    end_byte = body["end_byte"]
                    chunk_size = body["chunk_size"]

                    # Fetch the chunk from S3
                    chunk_content = get_object_content(
                        bucket_name,
                        object_key,
                        start_byte=start_byte,
                        end_byte=end_byte
                    )
                    # Process the content
                    chunk_word_count = word_count(chunk_content)
                    print(f"Processed chunk: {chunk_file_name}, Word Count: {chunk_word_count}")

                    end_time_msg = time.time()
                    elapsed_time = end_time_msg - start_time_msg
                    process_time += elapsed_time

                    # Generate a unique hash for deduplication
                    hash_id = generate_sha256_hash(
                        bucket_name,
                        object_key,
                        chunk_word_count,
                        chunk_file_name,
                        chunk_file_name,
                        queue_size,
                        max_line_per_chunk
                    )

                    # Send the word count to the result queue
                    if bucket_name and object_key:
                        result_message = {
                            "bucket_name": bucket_name,
                            "object_key": object_key,
                            "chunk_word_count": chunk_word_count,
                            "chunk_file_name": chunk_file_name,
                            "process_time_per_message": elapsed_time,
                            "queue_size": queue_size,
                            "max_line_per_chunk": max_line_per_chunk,
                            "ec2_id": ec2_name,
                            "hash_id": hash_id
                        }
                        send_message_to_sqs(result_queue, result_message, hash_id)
                        hash_ids.append(result_message)
                    else:
                        print("No chunks were processed; no result to send.")
                    # ---- If we reached this point, message processing succeeded ----
                    delete_message_from_sqs(process_queue, message['ReceiptHandle'])
                    chunk_count += 1
                    break  # Exit the retry loop

                except Exception as e:
                    print(f"Error processing message: {str(e)}")


    # Log the hash IDs to a file and upload to S3
    log_file_name = f"hash_log_{ec2_count}_{queue_size}_{max_line_per_chunk}_{ec2_name}.json"
    with open(log_file_name, "w") as log_file:
        json.dump(hash_ids, log_file)  # Save hash IDs as a JSON file
    print(f"Log file {log_file_name} created.")

    # Upload the log file to S3
    upload_file(log_file_name, "cst-cc-24-logs", f"{log_file_name}")
    print(f"File {log_file_name} successfully uploaded to S3.")

    # Clean up the local log file
    os.remove(log_file_name)

    # Measure end time
    print(f"All chunks were processed — total of {chunk_count} chunks in {process_time:.2f}s.")