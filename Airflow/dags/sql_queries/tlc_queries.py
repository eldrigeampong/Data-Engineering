from credentials import SF_ROLE, STORAGE_AWS_ROLE_ARN

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


create_passenger_count_dim = """
                             CREATE OR REPLACE TABLE passenger_count_dim (
                                                                           passenger_count_id INT PRIMARY KEY,
                                                                           passenger_count INT
                                                                         )
                             STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                             COMMENT = 'create passenger count dimension'

                             """


create_trip_distance_dim = """
                           CREATE OR REPLACE TABLE trip_distance_dim (
                                                                       trip_distance_id INT PRIMARY KEY,
                                                                       trip_distance FLOAT
                                                                     )
                           STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                           COMMENT = 'create trip distance dimension'

                           """


create_pickup_location_dim = """
                             CREATE OR REPLACE TABLE pickup_location_dim (
                                                                             pickup_location_id INT PRIMARY KEY,
                                                                             PULocationID INT,
                                                                             Borough TEXT,
                                                                             Zone TEXT,
                                                                             service_zone TEXT
                                                                           )
                             STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                             COMMENT = 'create pickup location dimension'

                           """


create_dropoff_location_dim = """
                              CREATE OR REPLACE TABLE dropoff_location_dim (
                                                                             dropoff_location_id INT PRIMARY KEY,
                                                                             DOLocationID INT
                                                                           )
                              STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                              COMMENT = 'create dropoff location dimension'

                           """


create_datetime_dim = """
                      CREATE OR REPLACE TABLE datetime_dim (
                                                             datetime_id INT PRIMARY KEY,
                                                             tpep_pickup_datetime	TIMESTAMP_NTZ,
                                                             tpep_dropoff_datetime TIMESTAMP_NTZ
                                                           )
                      STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                      COMMENT = 'create datetime dimension'

                      """


create_rate_code_dim = """
                       CREATE OR REPLACE TABLE rate_code_dim (
                                                                rate_code_id INT PRIMARY KEY,
                                                                RatecodeID INT,
                                                                rate_code_name TEXT
                                                              )
                       STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                       COMMENT = 'create rate code dimension'

                      """


create_payment_type_dim = """
                          CREATE OR REPLACE TABLE payment_type_dim (
                                                                     payment_type_id INT PRIMARY KEY,
                                                                     payment_type INT,
                                                                     payment_type_name TEXT
                                                                   )
                          STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                          COMMENT = 'create payment type dimension'

                          """


create_fact = """
              CREATE OR REPLACE TABLE fact_table (
                                                   VendorID INT PRIMARY KEY,
                                                   datetime_id INT REFERENCES datetime_dim (datetime_id),	
                                                   passenger_count_id	INT REFERENCES passenger_count_dim (passenger_count_id),
                                                   trip_distance_id INT REFERENCES trip_distance_dim (trip_distance_id),	
                                                   pickup_location_id INT REFERENCES pickup_location_dim (pickup_location_id),
                                                   dropoff_location_id INT REFERENCES dropoff_location_dim (dropoff_location_id),
                                                   rate_code_id INT REFERENCES rate_code_dim (rate_code_id),
                                                   payment_type_id INT REFERENCES payment_type_dim (payment_type_id)
                                                 )
              STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
              COMMENT = 'create fact table'

              """


create_aws_s3_integration = f"""
                             CREATE STORAGE INTEGRATION IF NOT EXISTS tlc_aws_s3_int
                             TYPE = EXTERNAL_STAGE
                             STORAGE_PROVIDER = 'S3'
                             ENABLED = TRUE
                             STORAGE_AWS_ROLE_ARN = '{STORAGE_AWS_ROLE_ARN}'
                             STORAGE_ALLOWED_LOCATIONS = ('s3://nyc-tlc-trip-data')
                             COMMENT = 'create an aws storage integration for nyc taxi trip data'

                            """


grant_permission_to_aws_s3_integration = f"""
                                          GRANT USAGE ON INTEGRATION tlc_aws_s3_int TO ROLE {SF_ROLE}

                                          """