import boto3

sts_client = boto3.client('sts')
                 
assumed_role_object=sts_client.assume_role(
    RoleArn="arn:aws:iam::420933651491:role/svcJenkins-WeedWhackers"
    # RoleSessionName="s3ToSnowflakeAssumeRoleSession"
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
