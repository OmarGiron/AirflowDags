import datetime
import logging
import os 
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.decorators import task
from sqlalchemy import sql

S3_BUCKET = Variable.get("S3_USER_PURCHASE_BUCKET")
S3_KEY = Variable.get("S3_USER_PURCHASE_KEY")
POSTGRES_TABLE = Variable.get("POSTGRES_USER_PURCHASE_TABLE")
POSTGRES_SCHEMA = Variable.get("POSTGRES_USER_PURCHASE_TABLE_SCHEMA")
FILE_PATH_INPUT = Variable.get("FILE_PATH_INPUT")
FILE_PATH_SQL = Variable.get("FILE_PATH_SQL")
INPUT_FILE_NAME = FILE_PATH_INPUT + os.sep + S3_KEY

with DAG(
    dag_id="DeBcamp_SetEnv_CopyFile"
    ,start_date=datetime.datetime(2020, 2, 2)
    ,schedule_interval="@once"
    ,catchup=False
    ,template_searchpath= FILE_PATH_SQL
) as dag:   

    # [START set_up_postgres_db]    
    set_up_postgres_db = PostgresOperator(
        task_id="set_up_postgres_db",
        sql="set_up_database.sql"        
    )
    # [END set_up_postgres_db]

    # [START get_file_from_s3]
    @task(task_id="get_file_from_s3") 
    def download_file_from_s3():
        s3hook = S3Hook(aws_conn_id="s3_conn")        
        user_purchase_data = s3hook.get_key(key=S3_KEY, bucket_name=S3_BUCKET)                
        
        logging.info("Downloading file: " + INPUT_FILE_NAME)
        user_purchase_data.download_file(INPUT_FILE_NAME)
        logging.info("File downloaded")
    # [END get_file_from_s3]        

    # [START load_user_purchase_to_postgres]
    @task(task_id="load_user_purchase_to_postgres") 
    def upload_File_To_Postgres(): 
        tablename = POSTGRES_SCHEMA + "." + POSTGRES_TABLE
        sql_command = "COPY " + tablename + " FROM STDIN (FORMAT CSV, DELIMITER ',' , HEADER ) "        

        pgHook = PostgresHook()        
        pgHook.copy_expert(sql_command, INPUT_FILE_NAME)        
    
    get_file_from_s3 = download_file_from_s3()
    load_user_purchase_to_postgres = upload_File_To_Postgres()

    set_up_postgres_db >> get_file_from_s3 >> load_user_purchase_to_postgres