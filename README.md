# Automation-for-Identifying-and-Reporting-Stale-EBS-Volumes-AWS-lambda-SNS-project


## For more projects, check out  
[https://harishnshetty.github.io/projects.html](https://harishnshetty.github.io/projects.html)

[![Video Tutorial](https://github.com/harishnshetty/image-data-project/blob/ccd5b46f956ad4cea6b16d3e03c9b2a236ecb107/stale-ebs.jpg)](https://youtu.be/BgyYqUXuHuk?si=Gi6vkxhnVJQBILkG)



### lambda time out
5 min  
Runtime python3.14

### IAM ROLE inline policy

- "ec2:DescribeSnapshots",
- "ec2:DescribeRegions",
- "ec2:DescribeVolumes",
- "ec2:DeleteVolume"
- "sns:Publish"


## IAM ROLE permissions
- CloudWatchFullAccess
- CloudWatchFullAccessV2


### start scheduler
`00 8 ? JAN-DEC SUN *`

### Delete the resources

1. delete the lambda function
2. delete the iam role
3. delete the event bridge scheduler
4. delete the sns topic
5. delete the ebs volumes