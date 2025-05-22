from src.local import run_local
from src.ec2_module import get_ec2_info
from src.sqs_module import purge_all_queues, send_message_to_sqs
from src.central_control_module import run_local_control
import time

def test_processing(max_line_per_chunks, queue_sizes):
    info_queue = ["info_queue_1","info_queue_2","info_queue_3","info_queue_4"]
    process_queue = "process_queue"
    # result_queue = "result_queue.fifo"
    result_queue = "result_queue"
    wait_time = 20

    # Define the output CSV file
    output_file = "test_results"
    node_running = len(get_ec2_info())  # Number of EC2 running
    output_file_path = f"{output_file}_on_{node_running}_ec2.csv"

    # Initialize the CSV file
    initialize_csv(output_file_path)
    for max_line_per_chunk in max_line_per_chunks:
        for queue_size in queue_sizes:
            print("Running on a local machine...")
            run_local_control(update_flag=False)
            print(f"Testing with chunk_size={max_line_per_chunk}, queue_size={queue_size}...")

            # Run local processing
            result = run_local(
                max_line_per_chunk,
                queue_size,
                wait_time,
                info_queue=info_queue,
                process_queue=process_queue,
                result_queue=result_queue
            )
            row = {
                "max_line_per_chunk": max_line_per_chunk,
                "queue_size": queue_size,
                "file_fetch_time": round(result.get("file_fetch_time", None), 2),
                "split_time": round(result.get("split_time", None), 2),
                "chunk_send_time": round(result.get("chunk_send_time", None), 2),
                "local_wait_ec2_process_time": round(result.get("local_wait_ec2_process_time", None), 2),
                "local_process_time": round(result.get("local_process_time", None), 2),
                "local_total_time": round(result.get("local_total_time", None), 2),
                "ec2_avg_time": round(result.get("avg_time_ec2", None), 2),

                "ec2_count": result.get("total_ec2", None),
                "chunk_count": result.get("chunk_count", None),
                "chunk_process": result.get("chunk_process", None),
                "validation_status": result.get("validation_status", None),
                "word_count": result.get("word_count", None),
                "baseline_word_count": result.get("baseline_word_count", None)
            }
            append_to_csv(output_file_path, row)
            time.sleep(5)
            # purge_all_queues()  # Purge all queues


    print("Test completed and results saved.")
    print("Stopping processes.")
    run_param = {
            "bucket_name": None,
            "object_key": None,
            "max_line_per_chunk": None,
            "queue_size": None,
            "wait_time_seconds": None,
        }
    for i in range(len(info_queue)):
        send_message_to_sqs(info_queue[i], run_param) # turn off still running ec2 application
    time.sleep(5)
    # purge_all_queues()  # Purge all queues

# Helper to initialize the CSV file with headers
def initialize_csv(output_file):
    import csv
    fieldnames = [
        "max_line_per_chunks", "queue_size", "file_fetch_time", "split_time",
        "chunk_send_time", "local_wait_ec2_process_time", "local_process_time", "local_total_time",
        "ec2_avg_time", "ec2_count", "chunk_count", "chunk_process", "validation_status",
        "word_count", "baseline_word_count"
    ]
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    print(f"Initialized CSV file: {output_file}")


# Helper to append a row to the CSV file
def append_to_csv(output_file, row):
    import csv
    with open(output_file, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=row.keys())
        writer.writerow(row)
    print(f"Appended row to CSV file: {row}")



# Define test parameters
max_line_per_chunks = [1500000, 1000000, 500000, 100000]

queue_sizes = [1, 5, 10]


# Run the test
if __name__ == "__main__":
    test_processing(max_line_per_chunks, queue_sizes)