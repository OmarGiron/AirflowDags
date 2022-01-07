import datetime
import logging
import os 
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.decorators import task
from s3_to_postgres import S3ToPostgresOperator

S3_BUCKET = Variable.get("S3_USER_PURCHASE_BUCKET")
S3_KEY = Variable.get("S3_USER_PURCHASE_KEY")
POSTGRES_TABLE = Variable.get("POSTGRES_USER_PURCHASE_TABLE")
POSTGRES_SCHEMA = Variable.get("POSTGRES_USER_PURCHASE_TABLE_SCHEMA")
FILE_PATH_INPUT = Variable.get("FILE_PATH_INPUT")
FILE_PATH_SQL = Variable.get("FILE_PATH_SQL")
FILE_NAME_SQL = Variable.get("FILE_NAME_SQL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
INPUT_FILE_NAME = FILE_PATH_INPUT + os.sep + S3_KEY

with DAG(
    dag_id="DeBcamp_SetEnv_Custom_Operator"
    ,start_date=datetime.datetime(2020, 2, 2)
    ,schedule_interval="@once"
    ,catchup=False
    ,template_searchpath= FILE_PATH_SQL
) as dag:   

    # [START set_up_postgres_db]    
    set_up_postgres_db = PostgresOperator(
        task_id="set_up_postgres_db",
        sql=FILE_NAME_SQL        
    )
    # [END set_up_postgres_db]         

    # [START load_user_purchase_to_postgres]
    load_user_purchase_to_postgres = S3ToPostgresOperator(
        task_id='transfer_s3_to_postgres',
        aws_conn_id = "s3_conn",
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        schema=POSTGRES_SCHEMA,
        table=POSTGRES_TABLE,
        copy_options=['csv']
        
    )
   # [END load_user_purchase_to_postgres]    

    set_up_postgres_db >>  load_user_purchase_to_postgres