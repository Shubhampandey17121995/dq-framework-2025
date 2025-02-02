from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
            StructField("ep_id", IntegerType(), False),
            StructField("rule_id", IntegerType(), False),
            StructField("entity_id", IntegerType(), False),
            StructField("column_name", StringType(), True),
            StructField("is_active", StringType(), True),
            StructField("parameter_value", StringType(), True),
            StructField("actual_value", StringType(), True),
            StructField("total_records", IntegerType(), False),
            StructField("failed_records_count", IntegerType(), False),
            StructField("er_status", StringType(), False),
            StructField("error_records_path", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("execution_timestamp", StringType(), False),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False)
            ])


VAR_S3_RULE_MASTER_PATH = "s3://dq-framework/dq_rule_master/"
VAR_S3_BUCKET_PATH = "//S3_BUCKET_PATH"
VAR_S3_ENTITY_MASTER_PATH = "s3://dq-framework/dq_entity_master/"
VAR_S3_EXECUTION_PLAN_PATH = "s3://dq-framework/dq_execution_plan/"

VAR_EXECUTION_RESULT_PATH = "s3://dq-framework/dq_execution_result/"
VAR_ERROR_RECORD_PATH = "s3://error_record_path/"

ENTITY_ID = "entity_001" #USER INPUT



# spark configuration variables
SPARK_CATALOG_NAME = "s3tablesbucket"
SPARK_CATALOG_IMPL = "software.amazon.s3tables.iceberg.S3TablesCatalog"
SPARK_CATALOG_WAREHOUSE = "arn:aws:s3tables:us-east-1:111122223333:bucket/amzn-s3-demo-table-bucket"
SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
SPARK_JARS_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
    "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3"
)
