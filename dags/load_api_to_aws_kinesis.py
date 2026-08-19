import json
import logging
from datetime import datetime
import boto3
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

api_base_url = 'https://jsonplaceholder.typicode.com'
kinesis_client = boto3.client('kinesis')   

## Setting up incremental user id for next call
def _set_api_user_id(**context):
    try:
        # Fetch current variable value inside execution time
        api_user_id = int(Variable.get("api_user_id", default_var=-1))
        logger.info(f'type:: {type(api_user_id)} and api_user_id:: {api_user_id}')
        
        if api_user_id == -1 or api_user_id == 10:
            # FIX: Passed value as string "1"
            new_id = "1"
        else:
            # FIX: Converted incremented int to str
            new_id = str(api_user_id + 1)
            
        Variable.set(key="api_user_id", value=new_id)  
        return f"Latest api user id {new_id} successfully updated"
    except Exception as e:
        logger.info(f'ERROR WHILE SETTING UP userId param value:: {e}')
        raise Exception(f'ERROR WHILE SETTING UP userId param value:: {e}')

def _extract_userposts(**context):
    try:
        # Dynamically read the newly updated variable value
        new_api_user_id = int(Variable.get("api_user_id", default_var=1))
        logger.info(f'type:: {type(new_api_user_id)} and new_api_user_id:: {new_api_user_id}')
        
        response = requests.get(f'{api_base_url}/posts?userId={new_api_user_id}')
        user_posts = response.json()
        logger.info(f'api data||user_posts:: {user_posts}')
        
        context['task_instance'].xcom_push(key='user_posts', value=user_posts)
        return user_posts
    except Exception as e:
        logger.info(f'ERROR WHILE FETCHING USER POSTS API DATA:: {e}')
        raise Exception(f'ERROR WHILE FETCHING USER POSTS API DATA:: {e}')

def _process_user_posts(**context):
    try:
        new_api_user_id = int(Variable.get("api_user_id", default_var=1))
        stream_name = "user-posts-data-stream"    
        user_posts = context['task_instance'].xcom_pull(task_ids='extract_userposts', key='user_posts')
        logger.info(f'api data|||user_posts:: {user_posts}')

        # Writing data one by one to kinesis data stream
        for user_post in user_posts:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(user_post) + '\n', 
                PartitionKey=str(user_post['userId']),
                SequenceNumberForOrdering=str(user_post['id'] - 1)
            )
            logger.info(
                f"Produced (Kinesis Data Stream) records {response['SequenceNumber']} "
                f"to Shard {response['ShardId']}, status code {response['ResponseMetadata']['HTTPStatusCode']} "
                f"and retry attempts count {response['ResponseMetadata']['RetryAttempts']}"
            )
    
        return f'Total {len(user_posts)} posts with user id {new_api_user_id} have been written into kinesis stream `{stream_name}`'
    except Exception as e:
        logger.info(f'ERROR WHILE WRITING USER POSTS TO KINESIS STREAM:: {e}')
        raise Exception(f'ERROR WHILE WRITING USER POSTS TO KINESIS STREAM:: {e}')


with DAG(
    dag_id='load_api_aws_kinesis', 
    default_args={'owner': 'Sovan'}, 
    tags=["api data load to s3"], 
    start_date=datetime(2023, 9, 24), 
    schedule='@daily', 
    catchup=False
):

    get_api_userId_params = PythonOperator(
        task_id='get_api_userId_params',
        python_callable=_set_api_user_id,
    ) 
    
    extract_userposts = PythonOperator(
        task_id='extract_userposts',
        python_callable=_extract_userposts,
    )

    write_userposts_to_stream = PythonOperator(
        task_id='write_userposts_to_stream',
        python_callable=_process_user_posts,
    )

    get_api_userId_params >> extract_userposts >> write_userposts_to_stream
