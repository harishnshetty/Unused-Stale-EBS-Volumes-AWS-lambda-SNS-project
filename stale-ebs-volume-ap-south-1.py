import boto3
import json
import time
from datetime import datetime, timedelta, timezone

sns = boto3.client('sns')
SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:970378220457:stale-ebs'  

ec2 = boto3.client('ec2')
cloudwatch = boto3.client('cloudwatch')

# Configuration flags
DRY_RUN = True # Set to True to test without deleting
NOTIFY_ONLY = False  # Set to True to only notify without deleting
STALE_DAYS_THRESHOLD = 7 # Days threshold for identifying stale volumes

def lambda_handler(event, context):
    # Step 1: Count total EBS volumes
    all_volumes = ec2.describe_volumes()
    total_count = len(all_volumes['Volumes'])

    # Step 2: Get and count available (unattached) EBS volumes
    available_volumes = ec2.describe_volumes(
        Filters=[{'Name': 'status', 'Values': ['available']}]
    )
    available_count = len(available_volumes['Volumes'])

    # Calculate the threshold date
    current_time = datetime.now(timezone.utc)
    threshold_time = current_time - timedelta(days=STALE_DAYS_THRESHOLD)

    # Get list of stale EBS volume IDs (created more than 7 days ago)
    stale_volume_ids = []
    for vol in available_volumes['Volumes']:
        if vol['CreateTime'] < threshold_time:
            stale_volume_ids.append(vol['VolumeId'])

    stale_volume_list_str = '\n'.join(stale_volume_ids) if stale_volume_ids else "No stale volumes."

    # Step 3: Delete stale volumes based on flags
    deletion_results = []
    if not NOTIFY_ONLY and stale_volume_ids:
        for volume_id in stale_volume_ids:
            try:
                if not DRY_RUN:
                    # Actually delete the volume
                    ec2.delete_volume(VolumeId=volume_id)
                    deletion_results.append(f"{volume_id} - DELETED")
                else:
                    # Dry run - just log what would be deleted
                    deletion_results.append(f"{volume_id} - WOULD BE DELETED (Dry Run)")
            except Exception as e:
                deletion_results.append(f"{volume_id} - ERROR: {str(e)}")
    
    # Format deletion results for display
    deletion_results_str = '\n'.join(deletion_results) if deletion_results else "No volumes were deleted."
    
    # Step 4: Push custom metrics to CloudWatch
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

    # Step 5: Create widgets for dashboard (counts and text)
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
            "height": 3,
            "properties": {
                "markdown": f"### Stale EBS Volume IDs\n```\n{stale_volume_list_str}\n```"
            }
        },
        {
            "type": "text",
            "x": 0,
            "y": 9,
            "width": 12,
            "height": 3,
            "properties": {
                "markdown": f"### Deletion Results\n**Mode:** {'NOTIFY ONLY' if NOTIFY_ONLY else ('DRY RUN' if DRY_RUN else 'ACTIVE DELETION')}\n```\n{deletion_results_str}\n```"
            }
        }
    ]

    # Step 6: Email report using SNS
    email_body = f"""Stale EBS Volume Report

Execution Mode: {'NOTIFY ONLY' if NOTIFY_ONLY else ('DRY RUN' if DRY_RUN else 'ACTIVE DELETION')}

Total EBS Volumes: {total_count}
Available (Unattached) Volumes: {available_count}
Stale Volumes (> {STALE_DAYS_THRESHOLD} days): {len(stale_volume_ids)}

Stale Volume IDs:
{stale_volume_list_str}

Deletion Results:
{deletion_results_str}
"""

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"Stale EBS Volume Report - Mode: {'NOTIFY ONLY' if NOTIFY_ONLY else ('DRY RUN' if DRY_RUN else 'ACTIVE')}",
        Message=email_body
    )

    # Step 7: Update the dashboard
    dashboard_body = json.dumps({"widgets": widgets})

    cloudwatch.put_dashboard(
        DashboardName="EBSVolumeDashboard",
        DashboardBody=dashboard_body
    )

    return {
        'statusCode': 200,
        'body': f'Dashboard updated. Total: {total_count}, Available: {available_count}, Mode: {"NOTIFY ONLY" if NOTIFY_ONLY else ("DRY RUN" if DRY_RUN else "ACTIVE DELETION")}'
    }