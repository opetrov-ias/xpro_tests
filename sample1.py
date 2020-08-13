import boto3
import pprint

client = boto3.client('sts')
                 
assumed_role_object=client.assume_role(
    RoleArn="arn:aws:iam::972380794107:role/svcJenkins-WeedWhackers",
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
