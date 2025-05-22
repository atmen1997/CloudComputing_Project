import os
import hashlib

# Function to enlarge a file by repeating its content
def enlarge(file_name, multiplier=10):
    try:
        base_name = os.path.basename(file_name).split(".")[0]
        output_file = f"{base_name}_large.txt"
        with open(file_name, "r") as input_file:
            content = input_file.read()
        with open(output_file, "w") as output_file:
            output_file.writelines((content + "\n") * multiplier)
        return output_file.name
    except Exception as e:
        print(f"Error enlarging file: {e}")
        return None

def split_chunk(bucket_name, object_key, file_content, max_line_per_chunk=10):
    # Split the file into lines
    lines = file_content.splitlines()
    total_lines = len(lines)
    file_bytes = file_content.encode("utf-8")  # Encode content to bytes
    chunks = []

    # Track byte positions
    current_byte = 0
    start_line = 0
    file_name = object_key.split(".")[0].strip()

    while start_line < total_lines:
        # Determine the end line for this chunk
        end_line = min(start_line + max_line_per_chunk, total_lines)
        chunk_lines = lines[start_line:end_line]
        
        # Calculate the byte range and size for the chunk
        chunk_text = "\n".join(chunk_lines) + ("\n" if end_line < total_lines else "")
        chunk_bytes = chunk_text.encode("utf-8")  # Convert chunk to bytes
        chunk_byte_start = current_byte
        chunk_byte_end = chunk_byte_start + len(chunk_bytes)
        chunk_size = len(chunk_bytes)  # Byte size of the chunk


        chunk_file_name = f"{file_name}_{start_line}_to_{end_line}"

        # Prepare chunk metadata
        chunks.append({
            "bucket_name": bucket_name,
            "object_key": object_key,
            "chunk_file_name": chunk_file_name,
            "start_line": start_line,
            "end_line": end_line,
            "start_byte": chunk_byte_start,
            "end_byte": chunk_byte_end,
            "chunk_size": round(chunk_size / 1024 / 1024, 2)
        })

        # Update byte position and line range
        current_byte = chunk_byte_end
        start_line = end_line

    print(f"File processed into {len(chunks)} chunks.")
    return chunks

# Function to join a chunk of lines
def join_chunk(lines, start, end):
    return "\n".join(lines[start:end])

# Function to count words in a string
def word_count(list_text):
    return len(list_text.split())

# General-purpose hash function
# From https://www.geeksforgeeks.org/md5-hash-python/
def generate_sha256_hash(*args):
    concatenated_values = ":".join(map(str, args))  # Join all values with a separator
    return hashlib.sha256(concatenated_values.encode()).hexdigest()

def generate_md5_hash(*args):
    concatenated_values = ":".join(map(str, args))  # Join all values with a separator
    return hashlib.md5(concatenated_values.encode()).hexdigest()

def write_to_file(file_path, content, mode="w"):
    with open(file_path, mode, encoding="utf-8") as file:
        file.write(content)
    print(f"Content written to file: {file_path}")
    return True

def read_from_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        content = file.read()
    print(f"Content read from file: {file_path}")
    return content


