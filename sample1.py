import boto3

sts_client = boto3.client('sts')

role = "arn:aws:iam::{0}:role/svcJenkins-WeedWhackers".format('420933651491')
print('ROLE:{0}'.format(role))                    

assumed_role_object=sts_client.assume_role(
    RoleArn=role, 
    RoleSessionName="s3ToSnowflakeAssumeRoleSession"
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
