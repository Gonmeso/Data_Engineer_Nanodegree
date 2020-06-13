import configparser
import boto3
import re


session = boto3.Session(profile_name='personal')

class NoOutputFoundError(Exception):
    '''
    Custom exception for missing outputs
    '''
    pass


# Parse config to get data to create stack
cfg = configparser.ConfigParser()
cfg.read('dwh.cfg')
STACK_NAME = 'sparkify-stack-name'


def get_table_name(s):
    """
    Gets tables names from query to help track logging progress

    :param s: query to parse
    :return: the matching table name
    """
    m = re.search(r'(IF.*EXISTS) ([a-z]+)', s)
    return m.group(2)


def get_output_str(s):
    '''
    Retrive output using it's output key
    :param s: string resembling th OutputKey
    :return : the output value from the cloudformation stack
    '''

    cf = create_cf_client()
    response = cf.describe_stacks(StackName=STACK_NAME)

    try:
        for output in response['Stacks'][0]['Outputs']:
            if output['OutputKey'] == s:
                print('---- Retrieving output {} with value "{}" ----'.format(
                    output['OutputKey'],
                    output['OutputValue']
                ))
                return output['OutputValue']
    except Exception:
        raise NoOutputFoundError


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


def get_cluster_connection_str():
    '''
    Fill the connection string using the configuration file
    and retriveng the cluster endpoint using boto3 and the
    cloudformation client

    :return : the connection string used by psycopg2
    '''

    conn_str = "host={} dbname={} user={} password={} port={}"
    endpoint = get_output_str('ClusterEndpoint')

    conn_str = conn_str.format(
        endpoint,
        'sparkify',
        'awsuser',
        'MasterPassword1',
        5439
    )

    return conn_str
