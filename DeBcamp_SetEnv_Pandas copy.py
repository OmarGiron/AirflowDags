import datetime
import io
import logging
import os
import pandas 
from os import getenv
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.decorators import task

# S3_BUCKET = getenv("S3_USER_PURCHASE_BUCKET", "de-bootcamp-userpurchase-local")
# S3_KEY = getenv("S3_USER_PURCHASE_KEY", "user_purchase.csv")
# POSTGRES_TABLE = getenv("USER_PURCHASE_TABLE", "user_purchase")
# POSTGRES_SCHEMA = getenv("MOVIES_SCHEMA", "movies")

S3_BUCKET = Variable.get("S3_USER_PURCHASE_BUCKET")
S3_KEY = Variable.get("S3_USER_PURCHASE_KEY")
POSTGRES_TABLE = Variable.get("POSTGRES_USER_PURCHASE_TABLE")
POSTGRES_SCHEMA = Variable.get("POSTGRES_USER_PURCHASE_TABLE_SCHEMA")
FILE_PATH_INPUT = Variable.get("FILE_PATH_INPUT")
FILE_PATH_SQL = Variable.get("FILE_PATH_SQL")

def file_path(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path

def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor()
    # CSV loading to table
    with open(file_path("/custom_modules/assets/cities_clean.csv"), "r") as f:
        next(f)
        curr.copy_from(f, 'cities', sep=",")
        get_postgres_conn.commit()

with DAG(
    dag_id="DeBcamp_SetEnv_Pandas"
    ,start_date=datetime.datetime(2020, 2, 2)
    ,schedule_interval="@once"
    ,catchup=False
    ,template_searchpath=FILE_PATH_SQL
    ,dagrun_timeout=datetime.timedelta(minutes=5)
) as dag:

    # [START create_movies_schema]
    # create_movies_schema = PostgresOperator(
    #     task_id="create_movies_schema",
    #     sql="CREATE SCHEMA IF NOT EXISTS  movies;"        
    # )
    # [END create_movies_schema]
    
    # [START create_user_purchase_table]
    # create_user_purchase_table = PostgresOperator(
    #     task_id="create_user_purchase_table",
    #     sql= """CREATE TABLE IF NOT EXISTS movies.user_purchase 
    #         (
    #         invoice_number varchar(10),
    #         stock_code varchar(20),
    #         detail varchar(1000),
    #         quantity int,
    #         invoice_date timestamp,
    #         unit_price numeric(8,3),
    #         customer_id int,
    #         country varchar(20)
    #         )
    #     ;"""        
    # )
    # [END create_user_purchase_table]

    # [START set_up_postgres_db]
    set_up_postgres_db = PostgresOperator(
        task_id="set_up_postgres_db",
        sql="set_up_database.sql"        
    )

    # [START load_user_purchase_info_from_s3_to_postgres]
    @task(task_id="load_user_purchase_info_from_s3_to_postgres") 
    def load_from_s3_to_postgres():
        s3hook = S3Hook(aws_conn_id="s3_conn")        
        user_purchase_data = s3hook.get_key(key=S3_KEY, bucket_name=S3_BUCKET).get()["Body"].read().decode("utf-8")
        
        #Define the datatypes of the columns to pass them to the pandas data frame
        schema = {
            "InvoiceNo" : "string"
            ,"StockCode" : "string"
            ,"Description" : "string"
            ,"Quantity" : "Int64"
            ,"InvoiceDate" : "object"
            ,"UnitPrice" : "float64"
            ,"CustomerID" : "Int64"
            ,"Country" : "string"
        }

        # Dictionary handle for Null values in the CSV
        replaceValuesForNulls = {            
            "Description" : ""            
            ,"CustomerID" : 0            
        }

        #Read the csv and store it in a pandas Data Frame
        df_user_purchase = pandas.read_csv(filepath_or_buffer = io.StringIO(user_purchase_data)
                                            ,delimiter=","
                                            ,header=0
                                            ,quotechar='"'
                                            ,low_memory=False
                                            ,dtype=schema
                                            ,parse_dates=["InvoiceDate"]                                            
                                            )
        
        #Replace null values
        df_user_purchase = df_user_purchase.fillna(replaceValuesForNulls)
        
        #Create a list of tuples to pass it to the postgres hook 
        user_purchase_tpls = [tuple(x) for x in df_user_purchase.to_numpy()]
        
        #Define the database columns
        target_fields = ["invoice_number"
                        ,"stock_code"
                        ,"detail"
                        ,"quantity"
                        ,"invoice_date"
                        ,"unit_price"
                        ,"customer_id"
                        ,"country"]
                
        pgHook = PostgresHook()
        table_Name = POSTGRES_SCHEMA+"."+POSTGRES_TABLE
        pgHook.insert_rows(table=table_Name
                        , rows=user_purchase_tpls
                        , target_fields=target_fields
                        , commit_every=1000
                        , replace=False)
    
    load_user_purchase_info_from_s3_to_postgres = load_from_s3_to_postgres()
    # [END load_user_purchase_info_from_s3_to_postgres]

    set_up_postgres_db >> load_user_purchase_info_from_s3_to_postgres