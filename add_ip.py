import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import requests
import getpass
from datetime import datetime


def get_public_ip():
    """
    Fetches the public IP address of the machine using an external service.

    :return: Public IP address as a string, or None if fetching fails.
    """
    try:
        response = requests.get('https://checkip.amazonaws.com/')
        response.raise_for_status()
        return response.text.strip()  # Return raw IP as a string
    except requests.RequestException:
        return None


def add_ip_to_security_group(ip_address, security_group_name, region='us-east-1'):
    """
    Adds an IP address to the ingress rules of a security group for PostgreSQL access and tags the rule with a descriptive name.

    :param ip_address: The IP address to allow, in CIDR format (e.g., "203.0.113.0/32").
    :param security_group_name: The name of the security group.
    :param region: AWS region where the security group exists.
    """
    try:
        ec2 = boto3.client('ec2', region_name=region)

        response = ec2.describe_security_groups(Filters=[{'Name': 'group-name', 'Values': [security_group_name]}])
        security_groups = response['SecurityGroups']

        if not security_groups:
            print(f"Security group '{security_group_name}' not found.")
            return

        security_group_id = security_groups[0]['GroupId']

        # Generate a descriptiveame description for the ingress rule
        username = getpass.getuser()
        current_date = datetime.now().strftime("%m/%d/%Y")
        rule_description = f"{username} access {current_date}"

        ingress_response = ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 5432,
                    'ToPort': 5432,
                    'IpRanges': [
                        {
                            'CidrIp': ip_address,
                            'Description': rule_description
                        }
                    ]
                }
            ]
        )

        # Tag the rule so we can find it later (see remove_developer_ips.py)
        if 'SecurityGroupRules' in ingress_response:
            rule_id = ingress_response['SecurityGroupRules'][0]['SecurityGroupRuleId']
            ec2.create_tags(
                Resources=[rule_id],
                Tags=[
                    {
                        'Key': 'Name',
                        'Value': 'Developer Access'
                    }
                ]
            )

        print(f"Added {ip_address} to the ingress rules of {security_group_name}")

    except NoCredentialsError:
        print("AWS credentials not found. Please configure your credentials.")
    except PartialCredentialsError:
        print("Incomplete AWS credentials found. Please check your configuration.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    ip_to_add = get_public_ip()
    if ip_to_add:
        ip_to_add += "/32"  # Convert to CIDR format for single IP
        security_group = "mirrulationsdb_security"
        aws_region = "us-east-1"

        add_ip_to_security_group(ip_to_add, security_group, aws_region)
    else:
        print("Could not retrieve the public IP address. Exiting.")
