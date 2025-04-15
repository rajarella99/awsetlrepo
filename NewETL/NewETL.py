import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# Function to execute Spark SQL queries
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    """
    Executes a Spark SQL query on the provided DynamicFrames.

    :param glueContext: GlueContext object
    :param query: SQL query string
    :param mapping: Dictionary mapping table aliases to DynamicFrames
    :param transformation_ctx: Transformation context name
    :return: Resulting DynamicFrame
    """
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from AWS Glue Data Catalog
AWSGlueDataCatalog_node = glueContext.create_dynamic_frame.from_catalog(
    database="taxi_dataset", 
    table_name="taxi_dataset_raw_taxi_dataset", 
    transformation_ctx="AWSGlueDataCatalog_node"
)

# SQL Query to transform the data
SqlQuery = '''
SELECT
    vendorid,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    -- Calculate trip duration in minutes
    (UNIX_TIMESTAMP(CAST(tpep_dropoff_datetime AS TIMESTAMP)) - UNIX_TIMESTAMP(CAST(tpep_pickup_datetime AS TIMESTAMP))) / 60 AS trip_duration_minutes
FROM
    taxi_dataset_raw_taxi_dataset
WHERE
    -- Filter out invalid rows
    fare_amount >= 0
    AND total_amount >= 0
    AND trip_distance > 0
    AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND UNIX_TIMESTAMP(CAST(tpep_dropoff_datetime AS TIMESTAMP)) > UNIX_TIMESTAMP(CAST(tpep_pickup_datetime AS TIMESTAMP));
'''

# Apply SQL query
TransformedData = sparkSqlQuery(
    glueContext, 
    query=SqlQuery, 
    mapping={"taxi_dataset_raw_taxi_dataset": AWSGlueDataCatalog_node}, 
    transformation_ctx="TransformedData"
)

# Write the transformed data to S3 in Parquet format
S3Sink = glueContext.write_dynamic_frame.from_options(
    frame=TransformedData,
    connection_type="s3",
    connection_options={
        "path": "s3://dileeplandingzone/etlprocesseddata/",
        "partitionKeys": []  # Add partition keys if needed
    },
    format="parquet",
    transformation_ctx="S3Sink"
)

# Commit the job
job.commit()