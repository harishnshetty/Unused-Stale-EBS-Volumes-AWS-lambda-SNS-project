import boto3
import json
import time
from datetime import datetime, timedelta

SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:970378220457:stale-ebs'  # Update as needed
REGION = 'ap-south-1'  # Region for SNS and Dashboard placement
DRY_RUN = False  # Set to True to test without deleting
NOTIFY_ONLY = False  # Set to True to only notify without deleting
# Configuration for S3
STALE_DAYS_THRESHOLD = 0  # Buckets with objects older than X days
EMPTY_BUCKETS_ONLY = True  # Only consider empty buckets
CHECK_OBJECT_LAST_MODIFIED = True  # Check when objects were last modified

cloudwatch_main = boto3.client('cloudwatch', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)

def is_bucket_stale(s3_client, bucket_name):
    """Check if a bucket is stale based on criteria"""
    
    # Check if bucket is empty
    try:
        # List objects in bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        
        if EMPTY_BUCKETS_ONLY:
            # Only consider empty buckets as stale
            return 'Contents' not in response or len(response.get('Contents', [])) == 0
        
        if CHECK_OBJECT_LAST_MODIFIED and 'Contents' in response:
            # Check last modified date of objects
            for obj in response.get('Contents', []):
                last_modified = obj['LastModified'].replace(tzinfo=None)
                days_old = (datetime.now() - last_modified).days
                if days_old < STALE_DAYS_THRESHOLD:
                    return False  # Bucket has recent objects
            return True  # All objects are stale
        
        return False
    except Exception as e:
        print(f"Error checking bucket {bucket_name}: {e}")
        return False

def lambda_handler(event, context):
    # S3 client (S3 is global, but we need region for other services)
    s3 = boto3.client('s3')
    
    total_buckets = 0
    stale_buckets_global = 0
    stale_bucket_names_all = []
    deleted_buckets = []

    widgets = []

    # Get all S3 buckets (S3 is global, not region-specific)
    try:
        response = s3.list_buckets()
        all_buckets = response['Buckets']
        total_buckets = len(all_buckets)
        
        for bucket in all_buckets:
            bucket_name = bucket['Name']
            creation_date = bucket['CreationDate']
            
            # Check if bucket is stale
            if is_bucket_stale(s3, bucket_name):
                stale_buckets_global += 1
                stale_bucket_names_all.append(f"{bucket_name} (Created: {creation_date})")
                
                # Delete stale buckets based on flags
                if not NOTIFY_ONLY:
                    try:
                        # IMPORTANT: Before deleting a bucket, it must be empty
                        # List and delete all objects first
                        if not DRY_RUN:
                            # Delete all objects in the bucket first
                            s3_resource = boto3.resource('s3')
                            bucket_resource = s3_resource.Bucket(bucket_name)
                            
                            # Delete all objects and versions
                            bucket_resource.objects.all().delete()
                            bucket_resource.object_versions.all().delete()
                            
                            # Now delete the bucket
                            s3.delete_bucket(Bucket=bucket_name)
                            deleted_message = f"{bucket_name} - DELETED"
                        else:
                            # Dry run - just log what would be deleted
                            deleted_message = f"{bucket_name} - WOULD BE DELETED (Dry Run)"
                        
                        deleted_buckets.append(deleted_message)
                    except Exception as e:
                        error_message = f"{bucket_name} - ERROR: {str(e)}"
                        deleted_buckets.append(error_message)
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error listing S3 buckets: {str(e)}'
        }

    # Push metrics to CloudWatch
    timestamp = time.time()
    cloudwatch_main.put_metric_data(
        Namespace='Custom/S3Metrics',
        MetricData=[
            {
                'MetricName': 'TotalBucketCount',
                'Value': total_buckets,
                'Unit': 'Count',
                'Timestamp': timestamp
            },
            {
                'MetricName': 'StaleBucketCount',
                'Value': stale_buckets_global,
                'Unit': 'Count',
                'Timestamp': timestamp
            }
        ]
    )

    # Create dashboard widgets
    stale_bucket_list_str = '\n'.join(stale_bucket_names_all) if stale_bucket_names_all else "No stale buckets."
    deletion_results_str = '\n'.join(deleted_buckets) if deleted_buckets else "No buckets were deleted."

    widgets = [
        {
            "type": "metric",
            "x": 0,
            "y": 0,
            "width": 6,
            "height": 6,
            "properties": {
                "metrics": [["Custom/S3Metrics", "TotalBucketCount"]],
                "view": "singleValue",
                "stat": "Average",
                "region": REGION,
                "title": "Total S3 Buckets"
            }
        },
        {
            "type": "metric",
            "x": 6,
            "y": 0,
            "width": 6,
            "height": 6,
            "properties": {
                "metrics": [["Custom/S3Metrics", "StaleBucketCount"]],
                "view": "singleValue",
                "stat": "Average",
                "region": REGION,
                "title": "Stale S3 Buckets"
            }
        },
        {
            "type": "text",
            "x": 0,
            "y": 6,
            "width": 12,
            "height": 3,
            "properties": {
                "markdown": f"### Stale S3 Bucket Names\n```\n{stale_bucket_list_str}\n```"
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

    # Email body
    email_body = f"""Stale S3 Bucket Report

Execution Mode: {'NOTIFY ONLY' if NOTIFY_ONLY else ('DRY RUN' if DRY_RUN else 'ACTIVE DELETION')}
Criteria: {'Empty buckets only' if EMPTY_BUCKETS_ONLY else f'Buckets with objects older than {STALE_DAYS_THRESHOLD} days'}

Total S3 Buckets: {total_buckets}
Stale Buckets: {stale_buckets_global}

Stale Bucket Names:
{stale_bucket_list_str}

Deletion Results:
{deletion_results_str}
"""

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"Stale S3 Bucket Report - Mode: {'NOTIFY ONLY' if NOTIFY_ONLY else ('DRY RUN' if DRY_RUN else 'ACTIVE')}",
        Message=email_body
    )

    # Publish dashboard
    dashboard_body = json.dumps({"widgets": widgets})
    cloudwatch_main.put_dashboard(
        DashboardName="Global-S3BucketDashboard",
        DashboardBody=dashboard_body
    )

    return {
        'statusCode': 200,
        'body': f'Dashboard updated. Total: {total_buckets}, Stale: {stale_buckets_global}, Mode: {"NOTIFY ONLY" if NOTIFY_ONLY else ("DRY RUN" if DRY_RUN else "ACTIVE DELETION")}'
    }