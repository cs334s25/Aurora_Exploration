import boto3
from datetime import datetime, timedelta, timezone


def get_active_connections(db_cluster_identifier, region='us-east-1'):
    cloudwatch = boto3.client('cloudwatch', region_name=region)

    # Define the time range for the metric query
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=5)  # Check the last 5 minutes

    response = cloudwatch.get_metric_statistics(
        Namespace='AWS/RDS',
        MetricName='DatabaseConnections',
        Dimensions=[
            {
                'Name': 'DBClusterIdentifier',
                'Value': db_cluster_identifier
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=60,  # 1-minute granularity
        Statistics=['Average']
    )

    # Extract and return the average connections
    if 'Datapoints' in response and response['Datapoints']:
        # Sort datapoints by timestamp and get the latest one
        latest_data = sorted(response['Datapoints'], key=lambda x: x['Timestamp'], reverse=True)[0]
        return latest_data['Average']
    else:
        # No data points mean zero active connections
        return 0


db_cluster_identifier = 'your-cluster-identifier'
region = 'us-east-1'
active_connections = get_active_connections(db_cluster_identifier, region)
print(f"Active connections: {active_connections}")
