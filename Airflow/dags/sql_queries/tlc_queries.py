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


create_storage_integration = f"""
                              CREATE STORAGE INTEGRATION IF NOT EXISTS airflow_tlc_aws_s3_int
                              TYPE = EXTERNAL_STAGE
                              STORAGE_PROVIDER = 'S3'
                              ENABLED = TRUE
                              STORAGE_AWS_ROLE_ARN = '{STORAGE_AWS_ROLE_ARN}'
                              STORAGE_ALLOWED_LOCATIONS = ('s3://nyc-tlc-trip-data/yellow-taxi/')
                              COMMENT = 'create an aws storage integration for nyc taxi trip data'

                              """


grant_integration_access = f"""
                            GRANT USAGE ON INTEGRATION airflow_tlc_aws_s3_int TO ROLE {SF_ROLE}

                            """


create_external_stage = """
                        CREATE STAGE IF NOT EXISTS airflow_tlc_aws_s3_stage
                        STORAGE_INTEGRATION = airflow_tlc_aws_s3_int
                        URL = 's3://nyc-tlc-trip-data/yellow-taxi/'
                        FILE_FORMAT = (TYPE = 'PARQUET')
                        COPY_OPTIONS = (ON_ERROR = 'ABORT_STATEMENT')
                        COMMENT = 'create an aws stage for nyc taxi trip data'

                        """


create_passenger_count_pipe = """
                              CREATE OR REPLACE PIPE passenger_count_dim_pipe
                              AUTO_INGEST = TRUE
                              COMMENT = 'Creates a passenger count dimension pipe'
                              AS
                              COPY INTO AIRFLOW_NYC_TLC.Airflow_Yellow_Taxi.passenger_count_dim
                              FROM @airflow_tlc_aws_s3_stage/passenger_count_dim
                              FILE_FORMAT = (TYPE = 'PARQUET')
                              MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE

                              """


create_trip_distance_pipe = """
                            CREATE OR REPLACE PIPE trip_distance_dim_pipe
                            AUTO_INGEST = TRUE
                            COMMENT = 'Creates a trip distance dimension pipe'
                            AS
                            COPY INTO AIRFLOW_NYC_TLC.Airflow_Yellow_Taxi.trip_distance_dim
                            FROM @airflow_tlc_aws_s3_stage/trip_distance_dim
                            FILE_FORMAT = (TYPE = 'PARQUET')
                            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE

                            """


create_pickup_location_pipe = """
                              CREATE OR REPLACE PIPE pickup_location_dim_pipe
                              AUTO_INGEST = TRUE
                              COMMENT = 'Creates a pickup location dimension pipe'
                              AS
                              COPY INTO AIRFLOW_NYC_TLC.Airflow_Yellow_Taxi.pickup_location_dim
                              FROM @airflow_tlc_aws_s3_stage/pickup_location_dim
                              FILE_FORMAT = (TYPE = 'PARQUET')
                              MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE

                              """


create_dropoff_location_pipe = """
                               CREATE OR REPLACE PIPE dropoff_location_dim_pipe
                               AUTO_INGEST = TRUE
                               COMMENT = 'Creates a dropoff location dimension pipe'
                               AS
                               COPY INTO AIRFLOW_NYC_TLC.Airflow_Yellow_Taxi.dropoff_location_dim
                               FROM @airflow_tlc_aws_s3_stage/dropoff_location_dim
                               FILE_FORMAT = (TYPE = 'PARQUET')
                               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                              
                               """


create_datetime_pipe = """
                       CREATE OR REPLACE PIPE datetime_dim_pipe
                       AUTO_INGEST = TRUE
                       COMMENT = 'Creates a datetime dimension pipe'
                       AS
                       COPY INTO AIRFLOW_NYC_TLC.Airflow_Yellow_Taxi.datetime_dim
                       FROM @airflow_tlc_aws_s3_stage/datetime_dim
                       FILE_FORMAT = (TYPE = 'PARQUET')
                       MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                              
                       """


create_rate_code_pipe = """
                        CREATE OR REPLACE PIPE rate_code_dim_pipe
                        AUTO_INGEST = TRUE
                        COMMENT = 'Creates a rate code dimension pipe'
                        AS
                        COPY INTO AIRFLOW_NYC_TLC.Airflow_Yellow_Taxi.rate_code_dim
                        FROM @airflow_tlc_aws_s3_stage/rate_code_dim
                        FILE_FORMAT = (TYPE = 'PARQUET')
                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                              
                        """


create_payment_type_pipe = """
                           CREATE OR REPLACE PIPE payment_type_dim_pipe
                           AUTO_INGEST = TRUE
                           COMMENT = 'Creates a payment type dimension pipe'
                           AS
                           COPY INTO AIRFLOW_NYC_TLC.Airflow_Yellow_Taxi.payment_type_dim
                           FROM @airflow_tlc_aws_s3_stage/payment_type_dim
                           FILE_FORMAT = (TYPE = 'PARQUET')
                           MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                              
                           """


create_fact_pipe = """
                   CREATE OR REPLACE PIPE fact_table_pipe
                   AUTO_INGEST = TRUE
                   COMMENT = 'Creates a fact table pipe'
                   AS
                   COPY INTO AIRFLOW_NYC_TLC.Airflow_Yellow_Taxi.fact_table
                   FROM @airflow_tlc_aws_s3_stage/fact_table
                   FILE_FORMAT = (TYPE = 'PARQUET')
                   MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                              
                  """