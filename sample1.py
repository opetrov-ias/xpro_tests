import boto3
import pprint

client = boto3.client('sts')
pprint.pprint(client.__dict__)
account_id = client.get_caller_identity()["Account"]
print(account_id)
                 
assumed_role_object=client.assume_role(
    RoleArn="arn:aws:iam::420933651491:role/svcJenkins-WeedWhackers",
    RoleSessionName= "AssumeRoleSession1"  # "s3ToSnowflakeAssumeRoleSession"
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
