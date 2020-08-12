from ias_etl_common.runtime import get_runtime
from invoke import task
import boto3
from samples import roles
from pprint import pprint

@task
def role_list(context):
    print("print roles...")
    rl = roles.get_roles()
    pprint(rl)

@task
def user(context):
    pprint(roles.current_user())

@task
def current_user(context):
    client = boto3.client('sts')
    current_user = client.get_caller_identity() # .get('Account')
    pprint(current_user)

@task
def s3_list(context, prefix='pinterest'):
    client = boto3.client('s3')
    objects = client.list_objects(Bucket='partner-measured', Prefix=prefix)
    pprint(objects)
    #aor key in client.list_objects(Bucket='partner-measured')['Contents']:
    #    print(key['Key'])

@task
def s3_size(context, bucket='partner-measured', prefix='pinterest/raw_logs/2020/08/05'):
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket = bucket,Prefix = prefix)
    total_size = 0
    for i, page in enumerate(pages):
        print('page:{0}'.format(i))
        for obj in page['Contents']:
            # print(obj['Size'])
            total_size += obj['Size']
    pprint('Size: {0}'.format(total_size))

@task
def show_runtime(context):
    runtime = get_runtime()
    pprint(runtime.__dict__)

