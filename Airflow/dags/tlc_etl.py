from credentials import *
from pendulum import datetime
from airflow import DAG
from sqlalchemy.util import deprecations
deprecations.SILENCE_UBER_WARNING=1
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from sql_queries.tlc_queries import *


with DAG(dag_id="Nyc_Yellow_Taxi", schedule="@daily", start_date=datetime(2023, 9, 16, tz="Europe/Berlin"), catchup=False) as dag:
  
  create_warehouse = SnowflakeOperator(task_id="create_warehouse", 
                                       snowflake_conn_id="snowflake_default", 
                                       role=SF_ROLE, 
                                       sql=create_warehouse_query
                                     )
  
  create_database = SnowflakeOperator(task_id="create_database", 
                                      snowflake_conn_id="snowflake_default", 
                                      role=SF_ROLE, 
                                      sql=create_database_query, 
                                      warehouse="AIRFLOW_TLC_WH"
                                    )
  
  create_schema = SnowflakeOperator(task_id="create_schema", 
                                    snowflake_conn_id="snowflake_default", 
                                    role=SF_ROLE, 
                                    sql=create_schema_query, 
                                    warehouse="AIRFLOW_TLC_WH",
                                    database="AIRFLOW_NYC_TLC"
                                  )
  
  create_passenger_count_table = SnowflakeOperator(task_id="create_passenger_count_table", 
                                                   snowflake_conn_id="snowflake_default", 
                                                   role=SF_ROLE, 
                                                   sql=create_passenger_count_dim, 
                                                   warehouse="AIRFLOW_TLC_WH",
                                                   database="AIRFLOW_NYC_TLC",
                                                   schema="Airflow_Yellow_Taxi"
                                                 )
  
  create_trip_distance_table = SnowflakeOperator(task_id="create_trip_distance_table", 
                                                 snowflake_conn_id="snowflake_default", 
                                                 role=SF_ROLE, 
                                                 sql=create_trip_distance_dim, 
                                                 warehouse="AIRFLOW_TLC_WH",
                                                 database="AIRFLOW_NYC_TLC",
                                                 schema="Airflow_Yellow_Taxi"
                                               )
  
  create_pickup_location_table = SnowflakeOperator(task_id="create_pickup_location_table", 
                                                   snowflake_conn_id="snowflake_default", 
                                                   role=SF_ROLE, 
                                                   sql=create_pickup_location_dim, 
                                                   warehouse="AIRFLOW_TLC_WH",
                                                   database="AIRFLOW_NYC_TLC",
                                                   schema="Airflow_Yellow_Taxi"
                                                 )
  
  create_dropoff_location_table = SnowflakeOperator(task_id="create_dropoff_location_table", 
                                                    snowflake_conn_id="snowflake_default", 
                                                    role=SF_ROLE, 
                                                    sql=create_dropoff_location_dim, 
                                                    warehouse="AIRFLOW_TLC_WH",
                                                    database="AIRFLOW_NYC_TLC",
                                                    schema="Airflow_Yellow_Taxi"
                                                  )
  
  create_datetime_table = SnowflakeOperator(task_id="create_datetime_table", 
                                            snowflake_conn_id="snowflake_default", 
                                            role=SF_ROLE, 
                                            sql=create_datetime_dim, 
                                            warehouse="AIRFLOW_TLC_WH",
                                            database="AIRFLOW_NYC_TLC",
                                            schema="Airflow_Yellow_Taxi"
                                          )
  
  create_rate_code_table = SnowflakeOperator(task_id="create_rate_code_table", 
                                             snowflake_conn_id="snowflake_default", 
                                             role=SF_ROLE, 
                                             sql=create_rate_code_dim, 
                                             warehouse="AIRFLOW_TLC_WH",
                                             database="AIRFLOW_NYC_TLC",
                                             schema="Airflow_Yellow_Taxi"
                                          )
  

  create_payment_type_table = SnowflakeOperator(task_id="create_payment_type_table", 
                                                snowflake_conn_id="snowflake_default", 
                                                role=SF_ROLE, 
                                                sql=create_payment_type_dim, 
                                                warehouse="AIRFLOW_TLC_WH",
                                                database="AIRFLOW_NYC_TLC",
                                                schema="Airflow_Yellow_Taxi"
                                              )
  

  create_fact_table = SnowflakeOperator(task_id="create_fact_table", 
                                        snowflake_conn_id="snowflake_default", 
                                        role=SF_ROLE, 
                                        sql=create_fact, 
                                        warehouse="AIRFLOW_TLC_WH",
                                        database="AIRFLOW_NYC_TLC",
                                        schema="Airflow_Yellow_Taxi"
                                      )
  

  create_s3_integration = SnowflakeOperator(task_id="create_aws_s3_integration", 
                                            snowflake_conn_id="snowflake_default", 
                                            role=SF_ROLE, 
                                            sql=create_aws_s3_integration, 
                                            warehouse="AIRFLOW_TLC_WH",
                                            database="AIRFLOW_NYC_TLC",
                                            schema="Airflow_Yellow_Taxi"
                                          )
  

  grant_permission_to_s3 = SnowflakeOperator(task_id="grant_permission_to_aws_s3_integration", 
                                             snowflake_conn_id="snowflake_default", 
                                             role=SF_ROLE, 
                                             sql=grant_permission_to_aws_s3_integration, 
                                             warehouse="AIRFLOW_TLC_WH",
                                             database="AIRFLOW_NYC_TLC",
                                             schema="Airflow_Yellow_Taxi"
                                           )
  
  

  # set task dependencies
  create_warehouse >>\
  create_database >>\
  create_schema >>\
    [create_passenger_count_table, create_trip_distance_table, create_pickup_location_table, create_dropoff_location_table, create_datetime_table,\
      create_rate_code_table, create_payment_type_table, create_fact_table] >>\
  create_s3_integration >>\
  grant_permission_to_s3