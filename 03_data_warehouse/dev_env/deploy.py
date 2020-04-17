import configparser
import time
import boto3


# Parse config to get data to create stack
cfg = configparser.ConfigParser()
cfg.read('dwh.cfg')
STACK_NAME = cfg['CLOUDFORMATION']['STACK_NAME']

def get_aws_credentials():
    '''
    Retrieve AWS credentials for later use

    :returns : both access and secret key
    '''
    print('---- Retrieving credentials ----')
    access = cfg['AWS']['AWS_ACCESS_KEY']
    secret = cfg['AWS']['AWS_SECRET_KEY']
    region = cfg['AWS']['REGION']

    return access, secret, region

def create_stack_params():
    '''
    Generate cloudformation params to use in boto3 crete_stack

    :returns : a list of dictionaries formated for boto3 create_stack function
    '''
    print('---- Creating cloudformation parameters ----')
    params = [
        {'RoleNameParam': cfg['IAM']['ROLE_NAME']},
        {'ClusterId': cfg['CLUSTER']['CLUSTER_ID']},
        {'ClusterType': cfg['CLUSTER']['CLUSTER_TYPE']},
        {'NodeType': cfg['CLUSTER']['NODE_TYPE']},
        {'NumberOfNodes': cfg['CLUSTER']['NUMBER_OF_NODES']},
        {'DBname': cfg['CLUSTER']['DB_NAME']},
        {'MasterUser': cfg['CLUSTER']['DB_USER']},
        {'MasterPassword': cfg['CLUSTER']['DB_PASSWORD']},
    ]

    for idx, param in enumerate(params):
        (k, v), = param.items()
        print(f'{k} --> {v}')
        params[idx] = {
            'ParameterKey': k,
            'ParameterValue': v
        }

    return params


def create_cf_client():
    '''
    Creates the CloudFormation client using aws credentials

    :returns : cloudformation client ready to use
    '''
    access, secret, region = get_aws_credentials()
    cf = boto3.client(
        'cloudformation',
        region_name=region,
        aws_access_key_id=access,
        aws_secret_access_key=secret
        )
    
    return cf


def create_cf_stack():
    '''
    Start the creation of the stack in AWS
    '''

    cf = create_cf_client()

    with open('sparkify_cloudformation.yaml', 'r') as f:
        template = f.read()
    
    print('---- Creating Stack ----')
    cf.create_stack(
        StackName=STACK_NAME,
        Capabilities=['CAPABILITY_NAMED_IAM'],
        TemplateBody=template,
        Parameters=create_stack_params(),
    )


def check_if_complete(waiting_time):
    '''
    Checks wether the stack is completed
    '''

    cf = create_cf_client()

    while True:
        response = cf.describe_stacks(StackName=STACK_NAME)
        last_event = response['Stacks'][0]

        if 'ROLLBACK' in last_event['StackStatus']:
            print('*** Stack creation failed, check cause @ aws console ***')
            break
        elif last_event['StackStatus'] == 'CREATE_COMPLETE':
            print('---- Stack created succesfully ----')
            break
        else:
            print(f'---- Stack creation is still in progress retrying in {waiting_time} seconds ----')
            time.sleep(waiting_time)



if __name__ == '__main__':
    create_cf_stack()
    check_if_complete(15)
    
