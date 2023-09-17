create_warehouse_query = """
                         CREATE WAREHOUSE IF NOT EXISTS AIRFLOW_TLC_WH
                         WITH
                         WAREHOUSE_TYPE = 'STANDARD'
                         WAREHOUSE_SIZE = 'X-SMALL'
                         INITIALLY_SUSPENDED = TRUE
                         SCALING_POLICY = 'STANDARD'
                         AUTO_SUSPEND = 600
                         COMMENT = 'creates a new virtual warehouse in the system'

                         """     


create_database_query = """
                        CREATE DATABASE IF NOT EXISTS AIRFLOW_NYC_TLC
                        DATA_RETENTION_TIME_IN_DAYS = 90
                        COMMENT = 'NYC TLC Database'

                        """ 


create_schema_query = """
                      CREATE SCHEMA IF NOT EXISTS Airflow_Yellow_Taxi
                      DATA_RETENTION_TIME_IN_DAYS = 90
                      COMMENT = 'Yellow Taxi'

                      """