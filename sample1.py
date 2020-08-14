import boto3
import pprint
import pyvault
import os

boto3.set_stream_logger('botocore', level='DEBUG')

os.environ['AWS_ACCESS_KEY_ID'] = pyvault.get_value("/etl/prod/etl_aws/s3-integralads-data-reporting/aws_access_key_id")
os.environ['AWS_SECRET_ACCESS_KEY'] = pyvault.get_value("/etl/prod/etl_aws/s3-integralads-data-reporting/aws_secret_access_key")
print(os.environ['AWS_ACCESS_KEY_ID'])
print(os.environ['AWS_SECRET_ACCESS_KEY'])

client = boto3.client('sts')
 
assumed_role_object=client.assume_role(
    RoleArn="arn:aws:iam::420933651491:role/svcJenkins-WeedWhackers",
    RoleSessionName= "AssumeRoleSession1" 
)

# From the response that contains the assumed role, get the temporary
# credentials that can be used to make subsequent API calls
credentials=assumed_role_object['Credentials']
s3 = boto3.client(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken'],
)

for bucket in s3.buckets.all():
    print(bucket.name)
