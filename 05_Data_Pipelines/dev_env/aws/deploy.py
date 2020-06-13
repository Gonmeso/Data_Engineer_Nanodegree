import time
import boto3

session = boto3.Session(profile_name='personal')

def create_cf_client():
    '''
    Creates the CloudFormation client using aws credentials
    :returns : cloudformation client ready to use
    '''
    cf = session.client(
        'cloudformation',
        region_name='us-west-2',
        )

    return cf


def create_stack_params():
    '''
    Generate cloudformation params to use in boto3 crete_stack

    :returns : a list of dictionaries formated for boto3 create_stack function
    '''
    print('---- Creating cloudformation parameters ----')
    params = [
        {'RoleNameParam':'sparkify-dwh-role'},
        {'ClusterId': 'sparkify-cluster'},
        {'ClusterType': 'single-node'},
        {'NodeType': 'dc2.large'},
        {'NumberOfNodes': '1'},
        {'DBname': 'sparkify'},
        {'MasterUser': 'awsuser'},
        {'MasterPassword': 'MasterPassword1'},
    ]

    for idx, param in enumerate(params):
        (k, v), = param.items()
        print(f'{k} --> {v}')
        params[idx] = {
            'ParameterKey': k,
            'ParameterValue': v
        }

    return params


def create_cf_stack():
    '''
    Start the creation of the stack in AWS
    '''

    cf = create_cf_client()

    with open('sparkify_cloudformation.yaml', 'r') as f:
        template = f.read()

    print('---- Creating Stack ----')
    cf.create_stack(
        StackName='sparkify-stack-name',
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
        response = cf.describe_stacks(StackName='sparkify-stack-name')
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
