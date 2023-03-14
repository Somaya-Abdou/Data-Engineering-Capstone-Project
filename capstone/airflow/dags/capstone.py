from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadDimensionOperator, DataQualityOperator,DataQualityOperator2)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import create_tables_capstone


default_args = {
    'owner': 'Somaya',
    'start_date': days_ago(2),
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5) ,
    "catchup" : False    }

dag = DAG('capstone_dag',
          default_args=default_args,
          schedule_interval= '@daily',
          description='Load and transform data in Redshift with Airflow' 
         )
           
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_in_redshift = PostgresOperator(
    task_id="create_all_tables",
    sql=create_tables_capstone.Create_all_tables,
    postgres_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    dag=dag )

stage_sas_data_to_redshift = StageToRedshiftOperator(
    task_id='Stage_sas_data',
    dag=dag,
    table="data.staging_sas_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="capstonebucket123",
    s3_key="sas_data/",
    format_as = 'parquet' ,
    provide_context=True                           )


stage_country_to_redshift = StageToRedshiftOperator(
    task_id='Stage_country',
    dag=dag,
    table="data.staging_country",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="capstonebucket123",
    s3_key="country.csv/part-00000-065984f5-d7af-44ba-820e-a6c79b5a8791-c000.csv",
    format_as = 'csv',
    provide_context  = True                       )

stage_visa_to_redshift = StageToRedshiftOperator(
    task_id='Stage_visa',
    dag=dag,
    table="data.staging_visa",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="capstonebucket123",
    s3_key="visa.csv/part-00000-819c1534-7c11-4f21-9c61-b27c5b2e7e8b-c000.csv",
    format_as = 'csv',
    provide_context  = True                       )

stage_travel_to_redshift = StageToRedshiftOperator(
    task_id='Stage_travel',
    dag=dag,
    table="data.staging_travel",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="capstonebucket123",
    s3_key="travel.csv/part-00000-937ba271-3ca6-4e4f-9bbe-b9a382e83dfa-c000.csv",
    format_as = 'csv',
    provide_context  = True                       )

stage_address_to_redshift = StageToRedshiftOperator(
    task_id='Stage_address',
    dag=dag,
    table="data.staging_address",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="capstonebucket123",
    s3_key="address.csv/part-00000-fcb3402b-1911-496a-8172-065669c8d0db-c000.csv",
    format_as = 'csv',
    provide_context  = True                       )

load_immigrants_dimension_table = LoadDimensionOperator(
    task_id='Load_immigrants_dim_table',
    dag=dag,
    redshift_conn_id ="redshift",
    aws_credentials_id ="aws_credentials",
    sql_query = SqlQueries.load_immigrants_table  )   

load_address_dimension_table = LoadDimensionOperator(
    task_id='Load_address_dim_table',
    dag=dag,
    redshift_conn_id ="redshift",
    aws_credentials_id ="aws_credentials",
    sql_query = SqlQueries.load_address_table  ) 

load_date_dimension_table = LoadDimensionOperator(
    task_id='Load_date_dim_table',
    dag=dag,
    redshift_conn_id ="redshift",
    aws_credentials_id ="aws_credentials",
    sql_query = SqlQueries.load_date_table  )  


run_quality_checks_1 = DataQualityOperator(
    task_id='Run_data_quality_checks_1',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    tables = ['data.date','data.address','data.immigrants']   )

run_quality_checks_2 = DataQualityOperator2(
    task_id='Run_data_quality_checks_2',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    tables = ['data.date','data.address','data.immigrants']   )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator            >> create_tables_in_redshift
create_tables_in_redshift >> stage_sas_data_to_redshift
create_tables_in_redshift >> stage_visa_to_redshift
create_tables_in_redshift >> stage_travel_to_redshift
create_tables_in_redshift >> stage_address_to_redshift
create_tables_in_redshift >> stage_country_to_redshift

stage_sas_data_to_redshift >> load_immigrants_dimension_table
stage_visa_to_redshift     >> load_immigrants_dimension_table
stage_travel_to_redshift   >> load_immigrants_dimension_table
stage_address_to_redshift  >> load_immigrants_dimension_table
stage_country_to_redshift  >> load_immigrants_dimension_table

stage_sas_data_to_redshift >> load_address_dimension_table
stage_visa_to_redshift     >> load_address_dimension_table
stage_travel_to_redshift   >> load_address_dimension_table
stage_address_to_redshift  >> load_address_dimension_table
stage_country_to_redshift  >> load_address_dimension_table

stage_sas_data_to_redshift >> load_date_dimension_table

load_immigrants_dimension_table >> run_quality_checks_1
load_address_dimension_table    >> run_quality_checks_1
load_date_dimension_table       >> run_quality_checks_1

run_quality_checks_1 >> run_quality_checks_2

run_quality_checks_2 >> end_operator                   
