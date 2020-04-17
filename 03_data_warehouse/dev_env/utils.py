import configparser
import boto3
import re


class NoOutputFoundError(Exception):
    '''
    Custom exception for missing outputs
    '''
    pass


# Parse config to get data to create stack
cfg = configparser.ConfigParser()
cfg.read('dwh.cfg')
STACK_NAME = cfg['CLOUDFORMATION']['STACK_NAME']


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
    access, secret, region = get_aws_credentials()
    cf = boto3.client(
        'cloudformation',
        region_name=region,
        aws_access_key_id=access,
        aws_secret_access_key=secret
        )

    return cf


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
        cfg['CLUSTER']['DB_NAME'],
        cfg['CLUSTER']['DB_USER'],
        cfg['CLUSTER']['DB_PASSWORD'],
        5439
    )

    return conn_str
