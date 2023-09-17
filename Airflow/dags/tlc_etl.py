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
  
  
  # set task dependencies
  create_warehouse >> create_database >> create_schema