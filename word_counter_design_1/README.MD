# Go to correct folder
cd ~/../word_counter_1.
# Get credential 
Start lab --> Click AWS Details --> Click Show next to AWS CLI --> Copy the following into src.config.py.
# Update credential
Set update_flag to True in run_local_control() in main.py.
This will update all of the EC2 as well as the local credential.
# Update to run on EC2 through SSH
Set run_local_terminal_flag to True in run_local_control() in main.py.
This will run EC2 on terminal within having to go on aws.
# Set Param
Change max_line_per_chunk or queue_size to see different result.
Change file_size_multiplier and the upload flag to make the file bigger.
# Run script
python3 main.py