import time
import botocore
from utils import create_cf_client, STACK_NAME

def remove_cf_stack(stack, interval):

    cf = create_cf_client()
    cf.delete_stack(StackName=stack)

    while True:
        try:
            status = cf.describe_stacks(
                StackName=stack
                )['Stacks'][0]['StackStatus']
        except botocore.exceptions.ClientError:
            print('---- The stack has been deleted ----')
            break
        print(f'---- Status: {status} ----')
        print(f'---- Retrying in {interval} seconds ----')
        time.sleep(interval)

if __name__ == '__main__':
    remove_cf_stack(STACK_NAME, 20)