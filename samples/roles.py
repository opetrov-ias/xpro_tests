import boto3



def get_roles():

    client = boto3.client('iam')
    roles = client.list_roles()
    #role_list = roles['Roles']
    #for key in role_list:
    #    print(key['RoleName'])
    #    print(key['Arn'])
    return roles


def current_user():
    client = boto3.client('sts')
    current_user = client.get_caller_identity() # .get('Account')
    return current_user

