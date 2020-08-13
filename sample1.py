import boto3

sts_client = boto3.client('sts')
# Call the assume_role method of the STSConnection object and pass the role
# ARN and a role session name.

assumed_role_object=sts_client.assume_role(
# RoleArn="arn:aws:iam::972380794107:role/svcSnowflakeS3",
#  RoleArn="arn:aws:iam::633215444626:role/svcJenkins-Snowflake",
# prod = 420933651491
# prod-rep = 454457967641
    RoleArn="arn:aws:iam::{0}:role/svcJenkins-WeedWhackers".format('420933651491'),
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
