from src.local import run_local
from src.ec2 import run_ec2
from src.text_processing_module import enlarge
from src.s3_module import upload_file
from src.sqs_module import purge_all_queues, monitor_queues, get_messages_from_sqs, delete_message_from_sqs
from src.central_control_module import run_local_control
from src.config import WORKER_1_UUID, WORKER_2_UUID, WORKER_3_UUID, WORKER_4_UUID
import json
import time
import os

def main():
    file_path = "/sys/hypervisor/uuid"
    # info_queue = ["info_queue_1","info_queue_2","info_queue_3","info_queue_4"]
    info_queue = None # Info queue is being amed in run_local() no need to set
    process_queue = None
    result_queue = "result_queue"  # For deduplication
    max_line_per_chunk = 500000
    file_size_multiplier = 50
    queue_size = 5
    wait_time = 20

    # Automated
    if os.path.exists(file_path):  # Check if it's running on an EC2
        print("This code is running on an EC2")
        uuid_path = "/sys/hypervisor/uuid"
        with open(uuid_path, "r") as file:
            uuid = file.read().strip()
        if uuid == WORKER_1_UUID:
            info_queue = "info_queue_1"
            process_queue = "process_queue_1"
            ec2_name = "Worker_1"
        elif uuid == WORKER_2_UUID:
            info_queue = "info_queue_2"
            process_queue = "process_queue_2"
            ec2_name = "Worker_2"
        elif uuid == WORKER_3_UUID:
            info_queue = "info_queue_3"
            process_queue = "process_queue_3"
            ec2_name = "Worker_3"
        elif uuid == WORKER_4_UUID:
            info_queue = "info_queue_4"
            process_queue = "process_queue_4"
            ec2_name = "Worker_4"

        while True:
            info_message = None  # Reset and retry
            info_message_body = None
            # Monitor and process info queue
            monitor_queues(info_queue, 1, 5, 180)  # Adjusted sleep_time and timeout
            info_message = get_messages_from_sqs(info_queue, max_messages=1, wait_time_seconds=20)

            # Retry accessing the first message body (Some times the monitor gives out false positives)
            for attempt in range(4):
                try:
                    if info_message:  # Check if a message exists
                        break  # Exit loop if a message is found
                    else:
                        print(f"Attempt {attempt + 1}: No message found, retrying...")
                        monitor_queues(info_queue, 1, 5, 180)
                        info_message = get_messages_from_sqs(info_queue, max_messages=1, wait_time_seconds=5)
                        time.sleep(5)  # Give some time before re-checking
                except (IndexError, AttributeError) as e:
                    print(f"Error accessing message: {e}")
                    info_message = None  # Reset and retry
                    info_message_body = None
            info_message_body = info_message[0]
            # If a message body is retrieved, process it
            if info_message and info_message_body:
                try:
                    param_info = json.loads(info_message_body['Body'])
                    bucket_name = param_info["bucket_name"]
                    object_key = param_info["object_key"]
                    max_line_per_chunk = param_info["max_line_per_chunk"]
                    queue_size = param_info["queue_size"]
                    wait_time = param_info["wait_time_seconds"]
                    exact_process_chunk = param_info["exact_process_chunk"]
                    delete_message_from_sqs(info_queue, info_message_body['ReceiptHandle'])
                    print("Message deleted from info queue")
                    if exact_process_chunk == 0:
                        return
                    print(f"Extracted parameters: bucket_name={bucket_name}, object_key={object_key}, "
                          f"max_line_per_chunk={max_line_per_chunk}, exact_process_chunk={exact_process_chunk} , queue_size={queue_size}, wait_time={wait_time}")
                    run_ec2(ec2_name=ec2_name, max_line_per_chunk=max_line_per_chunk,queue_size=queue_size, wait_time=wait_time, exact_process_chunk=exact_process_chunk,
                            info_queue=info_queue, process_queue=process_queue, result_queue=result_queue)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON message: {e}")
            else:
                print("No messages found in the info queue.")
                print("Ending process. Exiting...")
                break
    else:
        # If running locally
        run_local_control(update_flag=True, run_local_terminal_flag=True)
        purge_all_queues()
        print("This code is running on local")

        # Optionally enlarge a file and upload it (currently disabled with 'if False')
        if False:
            file_name = enlarge("Shakespeare.txt", file_size_multiplier)
            upload_file(file_name, "cst-cc-24-raw")

        # Run local processing
        run_local(max_line_per_chunk, queue_size, wait_time, info_queue=info_queue, process_queue=process_queue ,result_queue=result_queue)
        return

if __name__ == "__main__":
    main()