import boto3
import json
import time

SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:970378220457:stale-ebs'  # Update as needed
REGION = 'ap-south-1'  # Region for SNS and Dashboard placement
cloudwatch_main = boto3.client('cloudwatch', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)

def lambda_handler(event, context):
    ec2_global = boto3.client('ec2', region_name=REGION)
    regions = [r['RegionName'] for r in ec2_global.describe_regions()['Regions']]

    total_volumes_global = 0
    stale_volumes_global = 0
    stale_volume_ids_all = []

    widgets = []

    for region in regions:
        ec2 = boto3.client('ec2', region_name=region)
        cloudwatch = boto3.client('cloudwatch', region_name=region)

        # Get all volumes
        all_volumes = ec2.describe_volumes()
        total_count = len(all_volumes['Volumes'])

        # Get unattached volumes
        available_volumes = ec2.describe_volumes(
            Filters=[{'Name': 'status', 'Values': ['available']}]
        )
        available_count = len(available_volumes['Volumes'])

        stale_ids = [vol['VolumeId'] for vol in available_volumes['Volumes']]
        stale_volume_ids_all.extend([f"{region}: {vid}" for vid in stale_ids])

        total_volumes_global += total_count
        stale_volumes_global += available_count

        timestamp = time.time()
        cloudwatch.put_metric_data(
            Namespace='Custom/EBSMetrics',
            MetricData=[
                {
                    'MetricName': 'TotalVolumeCount',
                    'Dimensions': [{'Name': 'Region', 'Value': region}],
                    'Value': total_count,
                    'Unit': 'Count',
                    'Timestamp': timestamp
                },
                {
                    'MetricName': 'AvailableVolumeCount',
                    'Dimensions': [{'Name': 'Region', 'Value': region}],
                    'Value': available_count,
                    'Unit': 'Count',
                    'Timestamp': timestamp
                }
            ]
        )

        # Add region widget
        widgets.append({
            "type": "metric",
            "x": 0,
            "y": len(widgets) * 6,
            "width": 6,
            "height": 6,
            "properties": {
                "metrics": [["Custom/EBSMetrics", "TotalVolumeCount", "Region", region]],
                "view": "singleValue",
                "stat": "Average",
                "region": region,
                "title": f"Total Volumes - {region}"
            }
        })

        widgets.append({
            "type": "metric",
            "x": 6,
            "y": len(widgets) * 6,
            "width": 6,
            "height": 6,
            "properties": {
                "metrics": [["Custom/EBSMetrics", "AvailableVolumeCount", "Region", region]],
                "view": "singleValue",
                "stat": "Average",
                "region": region,
                "title": f"Stale Volumes - {region}"
            }
        })

    # Text widget for all stale volume IDs
    stale_volume_list_str = '\n'.join(stale_volume_ids_all) if stale_volume_ids_all else "No stale volumes."

    widgets.append({
        "type": "text",
        "x": 0,
        "y": len(widgets) * 6,
        "width": 12,
        "height": 6,
        "properties": {
            "markdown": f"### Stale EBS Volume IDs Across Regions\n```\n{stale_volume_list_str}\n```"
        }
    })

    # Email body
    email_body = f"""Stale EBS Volume Report (Across All Regions)

Total EBS Volumes: {total_volumes_global}
Stale (Unattached) Volumes: {stale_volumes_global}

Volume IDs:
{stale_volume_list_str}
"""

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="Global Stale EBS Volume Report",
        Message=email_body
    )

    # Publish dashboard in default region
    dashboard_body = json.dumps({"widgets": widgets})
    cloudwatch_main.put_dashboard(
        DashboardName="Global-EBSVolumeDashboard",
        DashboardBody=dashboard_body
    )

    return {
        'statusCode': 200,
        'body': f'Dashboard updated. Total: {total_volumes_global}, Stale: {stale_volumes_global}'
    }
