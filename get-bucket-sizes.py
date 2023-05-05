import boto3
import pandas as pd
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

# Connect to CloudWatch
cloudwatch = boto3.client('cloudwatch')

# Connect to S3
s3 = boto3.resource('s3')

# Define a function to get the BucketSizeBytes metric data for a given bucket and storage type
def get_metric_data(bucket, storage_type):
    response = cloudwatch.get_metric_statistics(
        Namespace='AWS/S3',
        MetricName='BucketSizeBytes',
        Dimensions=[
            {'Name': 'BucketName', 'Value': bucket},
            {'Name': 'StorageType', 'Value': storage_type}
        ],
        StartTime=datetime.utcnow() - timedelta(days=3),
        EndTime=datetime.utcnow(),
        Period=86400,
        Statistics=['Maximum']
    )
    datapoints = response['Datapoints']
    if datapoints:
        return max([datapoint['Maximum'] for datapoint in datapoints])
    else:
        return 0

# Log before pulling the list of bucket names
logging.info("Getting list of bucket names...")

# Get all buckets in the account
buckets = [bucket.name for bucket in s3.buckets.all()]

# Prepare the MetricDataQueries for all the metrics
metric_data_queries = []
for bucket in buckets:
    logging.info(f"Working on bucket: {bucket}...")
    metric_data_queries.append(get_metric_data(bucket, 'StandardStorage'))
    metric_data_queries.append(get_metric_data(bucket, 'IntelligentTieringIAStorage'))
    metric_data_queries.append(get_metric_data(bucket, 'IntelligentTieringFAStorage'))
    metric_data_queries.append(get_metric_data(bucket, 'IntelligentTieringAIAStorage'))

# Parse the MetricData and sum up the bucket sizes
bucket_sizes = {}
for i in range(0, len(metric_data_queries), 4):
    bucket = buckets[i // 4]
    total_size = sum(metric_data_queries[i:i+4])
    bucket_sizes[bucket] = total_size

# Convert the results to a Pandas dataframe and display without truncation
df = pd.DataFrame.from_dict(bucket_sizes, orient='index', columns=['Size (Bytes)'])
df['Size (TBs)'] = df['Size (Bytes)'] / (1024 ** 4)
df = df[['Size (TBs)']].sort_values(by='Size (TBs)', ascending=False).head(10)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)
pd.set_option('display.float_format', '{:.2f}'.format)
print(df)
