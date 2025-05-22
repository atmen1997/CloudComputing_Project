from .config import credential
from .ec2_module import get_ec2_info
import os
import subprocess
import time

# Function to update local AWS credentials
def update_local_credential(credential, local_credential_path):
    credentials_file = os.path.expanduser(local_credential_path)
    with open(credentials_file, "w") as f:
        f.write(credential)
    print(f"Updated AWS credentials")

# Function to update credentials or directories on EC2 instances
def copy_to_ec2_(public_ips, pem_file, local_path, ec2_path):
    for public_ip in public_ips:
        print(f"Copying to instance with public IP: {public_ip}")
        try:
            rsync_command = [
                "rsync",
                "-avz",  # Archive mode (handles both files and directories), verbose, and compress
                "--exclude", ".env",  # Exclude the .env file
                "-e", f"ssh -i {pem_file} -o StrictHostKeyChecking=no",  # Use SSH with the PEM file
                local_path,  # Source path
                f"ec2-user@{public_ip}:{ec2_path}"  # Destination path
            ]
            
            # Execute the rsync command
            subprocess.run(rsync_command, check=True)
            print(f"Successfully copied to {public_ip}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to copy to {public_ip}: {e}")

# Function to open a terminal and run the SSH command
def open_terminal_and_run_ssh(public_ip, pem_file, script_path):
    ssh_command = f"ssh -i {pem_file} -o StrictHostKeyChecking=no ec2-user@{public_ip} 'python3 {script_path}; exit'"
    subprocess.Popen([
        "osascript", "-e",
        f'tell application "Terminal" to do script "{ssh_command}"'
    ])
    print(f"Opened terminal for {public_ip}")

def run_local_control(update_flag = False,run_local_terminal_flag = False):
    # Update local credentials
    local_credential_path = os.path.expanduser("~/.aws/credentials")
    ec2_credential_path = "~/.aws/credentials"
    # Set up parameters
    pem_file = os.path.expanduser("~/.ssh/labsuser.pem")
    script_path = "~/word_counter_v2/main.py"
    # # Copy credentials to EC2
    if update_flag == True:
        update_local_credential(credential, local_credential_path)
        public_ips = get_ec2_info()
        copy_to_ec2_(public_ips, pem_file, local_credential_path, ec2_credential_path)

        # # Update code directory
        local_code_path = os.path.abspath(".")  # Get current directory
        ec2_code_path = "~/"
        copy_to_ec2_(public_ips, pem_file, local_code_path, ec2_code_path) # Copy code directory
        if run_local_terminal_flag == True:
        # Loop through public IPs and open a terminal to run script
            for ip in public_ips:
                open_terminal_and_run_ssh(ip, pem_file, script_path)
                time.sleep(0.5)  # Optional: Add a small delay to ensure terminals open smoothly
    else:
        if run_local_terminal_flag == True:
            public_ips = get_ec2_info()
            # Loop through public IPs and open a terminal to run script
            for ip in public_ips:
                open_terminal_and_run_ssh(ip, pem_file, script_path)
                time.sleep(0.5)  # Optional: Add a small delay to ensure terminals open smoothly
