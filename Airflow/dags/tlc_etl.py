from credentials import *
from pendulum import datetime
from airflow import DAG
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
  

  create_external_storage = SnowflakeOperator(task_id="create_external_storage_integration", 
                                              snowflake_conn_id="snowflake_default", 
                                              role=SF_ROLE, 
                                              sql=create_storage_integration, 
                                              warehouse="AIRFLOW_TLC_WH",
                                              database="AIRFLOW_NYC_TLC",
                                              schema="Airflow_Yellow_Taxi"
                                            )
  

  grant_external_storage_access = SnowflakeOperator(task_id="grant_privilege_to_external_storage_integration", 
                                                    snowflake_conn_id="snowflake_default", 
                                                    role=SF_ROLE, 
                                                    sql=grant_integration_access, 
                                                    warehouse="AIRFLOW_TLC_WH",
                                                    database="AIRFLOW_NYC_TLC",
                                                    schema="Airflow_Yellow_Taxi"
                                                  )
  

  load_passenger_count_table = SnowflakeOperator(task_id="copy_data_into_passenger_count_table", 
                                                 snowflake_conn_id="snowflake_default", 
                                                 role=SF_ROLE, 
                                                 sql=create_passenger_count_pipe, 
                                                 warehouse="AIRFLOW_TLC_WH",
                                                 database="AIRFLOW_NYC_TLC",
                                                 schema="Airflow_Yellow_Taxi"
                                               )
  

  load_trip_distance_table = SnowflakeOperator(task_id="copy_data_into_trip_distance_table", 
                                               snowflake_conn_id="snowflake_default", 
                                               role=SF_ROLE, 
                                               sql=create_trip_distance_pipe, 
                                               warehouse="AIRFLOW_TLC_WH",
                                               database="AIRFLOW_NYC_TLC",
                                               schema="Airflow_Yellow_Taxi"
                                             )
  

  load_pickup_location_table = SnowflakeOperator(task_id="copy_data_into_pickup_location_table", 
                                                 snowflake_conn_id="snowflake_default", 
                                                 role=SF_ROLE, 
                                                 sql=create_pickup_location_pipe, 
                                                 warehouse="AIRFLOW_TLC_WH",
                                                 database="AIRFLOW_NYC_TLC",
                                                 schema="Airflow_Yellow_Taxi"
                                               )
  

  load_dropoff_location_table = SnowflakeOperator(task_id="copy_data_into_dropff_location_table", 
                                                  snowflake_conn_id="snowflake_default", 
                                                  role=SF_ROLE, 
                                                  sql=create_dropoff_location_pipe, 
                                                  warehouse="AIRFLOW_TLC_WH",
                                                  database="AIRFLOW_NYC_TLC",
                                                  schema="Airflow_Yellow_Taxi"
                                                )
  

  load_datetime_table = SnowflakeOperator(task_id="copy_data_into_datetime_table", 
                                          snowflake_conn_id="snowflake_default", 
                                          role=SF_ROLE, 
                                          sql=create_datetime_pipe, 
                                          warehouse="AIRFLOW_TLC_WH",
                                          database="AIRFLOW_NYC_TLC",
                                          schema="Airflow_Yellow_Taxi"
                                        ) 


  load_rate_code_table = SnowflakeOperator(task_id="copy_data_into_rate_code_table", 
                                           snowflake_conn_id="snowflake_default", 
                                           role=SF_ROLE, 
                                           sql=create_rate_code_pipe, 
                                           warehouse="AIRFLOW_TLC_WH",
                                           database="AIRFLOW_NYC_TLC",
                                           schema="Airflow_Yellow_Taxi"
                                         )  


  load_payment_type_table = SnowflakeOperator(task_id="copy_data_into_payment_type_table", 
                                              snowflake_conn_id="snowflake_default", 
                                              role=SF_ROLE, 
                                              sql=create_payment_type_pipe, 
                                              warehouse="AIRFLOW_TLC_WH",
                                              database="AIRFLOW_NYC_TLC",
                                              schema="Airflow_Yellow_Taxi"
                                            ) 


  load_fact_table = SnowflakeOperator(task_id="copy_data_into_fact_table", 
                                      snowflake_conn_id="snowflake_default", 
                                      role=SF_ROLE, 
                                      sql=create_fact_pipe, 
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
  create_external_storage >>\
  grant_external_storage_access >>\
    [load_passenger_count_table, load_trip_distance_table, load_pickup_location_table, load_dropoff_location_table, load_datetime_table,\
      load_rate_code_table, load_payment_type_table, load_fact_table]