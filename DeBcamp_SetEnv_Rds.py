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
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
INPUT_FILE_NAME = FILE_PATH_INPUT + os.sep + S3_KEY

with DAG(
    dag_id="DeBcamp_SetEnv_Rds_Postgres"
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

    # [START load_user_purchase_to_postgres]
    @task(task_id="load_user_purchase_to_postgres") 
    def upload_from_s3_to_postgres(): 
        tablename = POSTGRES_SCHEMA + "." + POSTGRES_TABLE        
        s3_uri = "s3://"+S3_BUCKET+"/"+S3_KEY
        sql_command = "SELECT aws_s3.table_import_from_s3('"+tablename+"', '', '(format csv)', :'"+s3_uri+"', "
        sql_command += " aws_commons.create_aws_credentials('"+S3_ACCESS_KEY+"', '"+S3_SECRET_KEY+"', '') );"

        pgHook = PostgresHook()        
        pgHook.run(sql_command)
   # [END load_user_purchase_to_postgres]
              
    load_user_purchase_to_postgres = upload_from_s3_to_postgres()

    set_up_postgres_db >>  load_user_purchase_to_postgres