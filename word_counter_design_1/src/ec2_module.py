import boto3

def get_ec2_info():
    ec2 = boto3.client("ec2")  # Initialize the EC2 client

    # Describe instances and filter by "running" state
    response = ec2.describe_instances(
        Filters=[
            {"Name": "instance-state-name", "Values": ["running"]}
        ]
    )
    # Collect public IPs of running instances
    public_ips = []
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            if "PublicIpAddress" in instance:
                public_ips.append(instance["PublicIpAddress"])

    return public_ips
