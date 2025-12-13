import boto3
import json
import time

sns = boto3.client('sns')
SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:970378220457:stale-ebs'  

ec2 = boto3.client('ec2')
cloudwatch = boto3.client('cloudwatch')

def lambda_handler(event, context):
    # Step 1: Count total EBS volumes
    all_volumes = ec2.describe_volumes()
    total_count = len(all_volumes['Volumes'])

    # Step 2: Get and count available (unattached) EBS volumes
    available_volumes = ec2.describe_volumes(
        Filters=[{'Name': 'status', 'Values': ['available']}]
    )
    available_count = len(available_volumes['Volumes'])

    # Get list of stale EBS volume IDs
    stale_volume_ids = [vol['VolumeId'] for vol in available_volumes['Volumes']]
    stale_volume_list_str = '\n'.join(stale_volume_ids) if stale_volume_ids else "No stale volumes."

    # Step 3: Push custom metrics to CloudWatch
    cloudwatch.put_metric_data(
        Namespace='Custom/EBSMetrics',
        MetricData=[
            {
                'MetricName': 'TotalVolumeCount',
                'Value': total_count,
                'Unit': 'Count',
                'Timestamp': time.time()
            },
            {
                'MetricName': 'AvailableVolumeCount',
                'Value': available_count,
                'Unit': 'Count',
                'Timestamp': time.time()
            }
        ]
    )

    # Step 4: Create widgets for dashboard (counts and text)
    widgets = [
        {
            "type": "metric",
            "x": 0,
            "y": 0,
            "width": 6,
            "height": 6,
            "properties": {
                "metrics": [["Custom/EBSMetrics", "TotalVolumeCount"]],
                "view": "singleValue",
                "stat": "Average",
                "region": "ap-south-1",
                "title": "Total EBS Volumes"
            }
        },
        {
            "type": "metric",
            "x": 6,
            "y": 0,
            "width": 6,
            "height": 6,
            "properties": {
                "metrics": [["Custom/EBSMetrics", "AvailableVolumeCount"]],
                "view": "singleValue",
                "stat": "Average",
                "region": "ap-south-1",
                "title": "Stale EBS Volumes"
            }
        },
        {
            "type": "text",
            "x": 0,
            "y": 6,
            "width": 12,
            "height": 6,
            "properties": {
                "markdown": f"### Stale EBS Volume IDs\n```\n{stale_volume_list_str}\n```"
            }
        }
    ]

    # Step 5: Email report using SNS
    email_body = f"""Stale EBS Volume Report

Total EBS Volumes: {total_count}
Stale (Unattached) Volumes: {available_count}

Volume IDs:
{stale_volume_list_str}
"""

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="Stale EBS Volume Report",
        Message=email_body
    )

    # Step 6: Update the dashboard
    dashboard_body = json.dumps({"widgets": widgets})

    cloudwatch.put_dashboard(
        DashboardName="EBSVolumeDashboard",
        DashboardBody=dashboard_body
    )

    return {
        'statusCode': 200,
        'body': f'Dashboard updated. Total: {total_count}, Available: {available_count}'
    }
