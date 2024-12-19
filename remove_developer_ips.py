import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


def remove_ip_from_security_group(security_group_name, region='us-east-1'):
    """
    Removes all ingress rules from the specified security group where the "Name" tag is "Developer Access".

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

        rules_response = ec2.describe_security_group_rules(Filters=[{'Name': 'group-id', 'Values': [security_group_id]}])

        for rule in rules_response['SecurityGroupRules']:
            rule_id = rule['SecurityGroupRuleId']

            tag_response = ec2.describe_tags(Filters=[
                {'Name': 'resource-id', 'Values': [rule_id]},
                {'Name': 'key', 'Values': ['Name']},
                {'Name': 'value', 'Values': ['Developer Access']}
            ])

            if tag_response['Tags']:
                ec2.revoke_security_group_ingress(
                    GroupId=security_group_id,
                    SecurityGroupRuleIds=[rule_id]
                )
                print(f"Removed rule {rule_id}: {rule['Description']}")

    except NoCredentialsError:
        print("AWS credentials not found. Please configure your credentials.")
    except PartialCredentialsError:
        print("Incomplete AWS credentials found. Please check your configuration.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    security_group = "mirrulationsdb_security"
    aws_region = "us-east-1"

    remove_ip_from_security_group(security_group, aws_region)
