from src.s3_module import get_object_content
from src.text_processing_module import split_chunk, word_count, write_to_file, read_from_file
from src.sqs_module import send_message_to_sqs, get_messages_from_sqs, monitor_queues, delete_message_from_sqs
from src.ec2_module import get_ec2_info
import time
import json
import os

def run_local(max_line_per_chunk, queue_size, wait_time_seconds, **queue_name):
    # Measure total time
    total_start_time = time.time()

    # Get location of object
    bucket_name, object_key = "cst-cc-24-raw", "Shakespeare_large.txt"

    # Get file start time
    file_fetch_start_time = time.time()

    # Get file end time
    file_fetch_end_time = time.time()
    file_fetch_elapsed_time = file_fetch_end_time - file_fetch_start_time


    # Count running EC2 instances and send object location info
    num_ec2_running = len(get_ec2_info())

    if os.path.exists(object_key):
        file_content = read_from_file(object_key)
    else:
        # Fetch object content
        file_content = get_object_content(bucket_name, object_key)
        write_to_file(object_key, file_content)

    # Baseline word count
    baseline_word_count = word_count(file_content)

    # Split content into chunks
    try:
        # Measure split start time
        split_start_time = time.time()
        chunks = split_chunk(bucket_name, object_key, file_content, max_line_per_chunk)
        num_chunks = len(chunks)
        # Measure split end time
        split_end_time = time.time()
        split_elapsed_time = split_end_time - split_start_time

        # Create queue names dynamically
        process_queue_name = [f"process_queue_{i+1}" for i in range(num_ec2_running)]
        info_queue_name = [f"info_queue_{i+1}" for i in range(num_ec2_running)]

        print(f"Distributing {num_chunks} chunks among {num_ec2_running} queues.")

        # Measure chunk send start time
        chunk_send_start_time = time.time()

        # Initialize a dictionary to track the chunks assigned to each queue
        queue_chunk_map = {queue: [] for queue in process_queue_name}

        # Distribute chunks across the queues
        for index, chunk in enumerate(chunks):
            # Round-robin distribution: choose the queue based on the chunk index
            selected_queue = process_queue_name[index % num_ec2_running]
            queue_chunk_map[selected_queue].append(chunk)
            send_message_to_sqs(selected_queue, chunk)

        print("All data sent to SQS.")

        # Measure chunk send end time
        chunk_send_end_time = time.time()
        chunk_send_elapsed_time = chunk_send_end_time - chunk_send_start_time

        # Calculate the exact number of chunks per queue
        exact_process_chunk = {queue: len(chunks) for queue, chunks in queue_chunk_map.items()}

        # Prepare the `run_param` for each queue
        run_params = []
        for queue, chunk_count in exact_process_chunk.items():
            run_params.append({
                "bucket_name": bucket_name,
                "object_key": object_key,
                "max_line_per_chunk": max_line_per_chunk,
                "queue_size": chunk_count,
                "wait_time_seconds": wait_time_seconds,
                "exact_process_chunk": chunk_count  # Exact number of chunks for this queue
            })

        # Send info to each `info_queue`
        for i, (queue, info_queue) in enumerate(zip(process_queue_name, info_queue_name)):
            run_param = {
                "bucket_name": bucket_name,
                "object_key": object_key,
                "max_line_per_chunk": max_line_per_chunk,
                "queue_size": queue_size,
                "wait_time_seconds": wait_time_seconds,
                "exact_process_chunk": exact_process_chunk[queue]
            }
            send_message_to_sqs(info_queue, run_param)
            print(f"Sent object location info to {info_queue} ({i + 1}/{num_ec2_running})")
            

    except Exception as e:
        print(f"Error processing or sending chunks: {e}")
        return {
            "error": f"Error during chunk processing: {e}"
        }

    print("Start processing")

    # Measure process start time
    ec2_wait_process_start_time = time.time()

    min_process_chunk = None
    if num_chunks // num_ec2_running < 1:
        min_process_chunk = 1
    else:
        min_process_chunk = num_chunks // num_ec2_running
    # Monitor result queue
    monitor_queues(queue_name["result_queue"], 
                   num_chunks, max(min_process_chunk//num_ec2_running,2), 
                   max(num_chunks,90))

    ec2_wait_process_end_time = time.time()
    ec2_wait_process_elapsed_time = ec2_wait_process_end_time - ec2_wait_process_start_time

    # Process messages from result queue
    word_counter = 0
    chunk_counter = 0
    ec2_ids = set()
    ec2_process_time_second = 0
    chunk_file_name = []
    # Measure process start time
    local_process_start_time = time.time()
    timeout =  max(num_chunks+180,90) # Set timeout duration in seconds
    loop_start_time = time.time()
    validation_status = None
    while True:
        current_time = time.time()
        loop_elapsed_time = current_time - loop_start_time

        # Break the loop if the timeout is reached
        if loop_elapsed_time > timeout:
            print(f"Timeout of {timeout} seconds reached.")
            break

        messages = get_messages_from_sqs(queue_name["result_queue"], queue_size, wait_time_seconds)
        if not messages and chunk_counter == num_chunks:
            validation_status = word_counter == baseline_word_count
            break

        elif chunk_counter > num_chunks:
            print(f"Too much chunk!")
            break

        for message in messages:
            try:
                body = json.loads(message['Body'])
                print(f"Received body: {body}")
                chunk_file_name.append(body["chunk_file_name"])
                chunk_word_count = body["chunk_word_count"]
                word_counter += int(chunk_word_count)
                chunk_counter += 1
                process_time = body["process_time_per_message"]
                ec2_process_time_second += float(process_time)
                ec2_ids.add(body["ec2_id"])
                print(f"Updated chuck count: {chunk_counter}")
                delete_message_from_sqs(queue_name["result_queue"],message["ReceiptHandle"])
                print("Message deleted from queue")
            except Exception as e:
                print(f"Error processing message: {e}")

    ec2_counter = len(ec2_ids)
    # Measure process end time
    local_process_end_time = time.time()
    local_process_elapsed_time = local_process_end_time - local_process_start_time

    # Calculate total elapsed time
    total_end_time = time.time()
    total_elapsed_time = total_end_time - total_start_time
    avg_time_ec2 = ec2_process_time_second/ec2_counter if ec2_counter != 0 else 0
    print(f"Word count: {word_counter}")
    print(f"Base word count: {baseline_word_count}")
    print(f"Time to fetch file: {file_fetch_elapsed_time:.2f} seconds")
    print(f"Time to split file into chunks: {split_elapsed_time:.2f} seconds")
    print(f"Time to send chunks to SQS: {chunk_send_elapsed_time:.2f} seconds")
    print(f"Time local wait for ec2 to processing: {ec2_wait_process_elapsed_time:.2f} seconds")
    print(f"Time local to processing: {local_process_elapsed_time:.2f} seconds")
    print(f"Average Time Elapsed Ec2: {avg_time_ec2:.2f} seconds")
    print(f"Total Time Elapsed Local: {total_elapsed_time:.2f} seconds")
    print(f"EC2 Processing: {ec2_counter}")
    print(f"EC2s: {ec2_ids}")
    print("Finshed processing!")

    return {
        "file_fetch_time": file_fetch_elapsed_time,
        "split_time": split_elapsed_time,
        "chunk_send_time": chunk_send_elapsed_time,
        "local_wait_ec2_process_time": ec2_wait_process_elapsed_time,
        "local_process_time": local_process_elapsed_time,
        "local_total_time": total_elapsed_time,
        "avg_time_ec2": avg_time_ec2,
        "total_ec2": ec2_counter,
        "chunk_count": num_chunks,
        "chunk_process": chunk_counter,
        "validation_status": validation_status if validation_status is not None else False,
        "word_count": word_counter,
        "baseline_word_count": baseline_word_count
    }

