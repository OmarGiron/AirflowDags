# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# [START postgres_operator_howto_guide]
import datetime
from os import getenv
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from s3_to_postgres import S3ToPostgresOperator


S3_BUCKET = getenv("S3_USER_PURCHASE_BUCKET", "de-bootcamp-userpurchase-local")
S3_KEY = getenv("S3_USER_PURCHASE_KEY", "user_purchase_sample.csv")
POSTGRES_TABLE = getenv("USER_PURCHASE_TABLE", "user_purchase")



with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_movies_schema = PostgresOperator(
        task_id="create_movies_schema",
        sql="CREATE SCHEMA IF NOT EXISTS  movies;"        
    )

    
    create_user_purchase_table = PostgresOperator(
        task_id="create_user_purchase_table",
        sql= """CREATE TABLE IF NOT EXISTS movies.user_purchase 
            (
            invoice_number varchar(10),
            stock_code varchar(20),
            detail varchar(1000),
            quantity int,
            invoice_date timestamp,
            unit_price numeric(8,3),
            customer_id int,
            country varchar(20)
            )
        ;"""        
    )

    transfer_s3_to_postgres = S3ToPostgresOperator(
        task_id='transfer_s3_to_postgres',
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        schema="movies",
        table=POSTGRES_TABLE,
        copy_options=['csv']        
    )

        

    create_movies_schema >> create_user_purchase_table >> transfer_s3_to_postgres
  