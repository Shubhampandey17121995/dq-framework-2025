import importlib.resources as pkg_resources
import json
import metadata
import importlib.resources as pkg_resources
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# schema to create the df of execution result
EXECUTION_RESULTS_SCHEMA = StructType([
            StructField("er_id", IntegerType(), False),
            StructField("ep_id", IntegerType(), False),
            StructField("rule_id", IntegerType(), False),
            StructField("entity_id", IntegerType(), False),
            StructField("column_name", StringType(), True),
            StructField("is_critical", StringType(), True),
            StructField("parameter_value", StringType(), True),
            StructField("total_records", IntegerType(), False),
            StructField("failed_records_count", IntegerType(), False),
            StructField("er_status", StringType(), False),
            StructField("error_records_path", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("execution_timestamp", StringType(), False),
            StructField("year", StringType(), False),
            StructField("month", StringType(), False),
            StructField("day", StringType(), False)
            ])

# entity id
VAR_ENTITY_ID = None

# config table paths
VAR_S3_RULE_MASTER_PATH = "s3tablesbucket.dq_testdb2.dq_rule_master"
VAR_S3_ENTITY_MASTER_PATH = "s3tablesbucket.dq_testdb2.dq_entity_master"
VAR_S3_EXECUTION_PLAN_PATH = "s3tablesbucket.dq_testdb2.dq_execution_plan"
VAR_S3_EXECUTION_RESULT_PATH = "s3tablesbucket.dq_testdb2.dq_execution_result"


#s3table bucket ARN id
MY_TABLE_BUCKET_ARN="arn:aws:s3tables:us-east-1:971996090633:bucket/dq-framework"


# result store paths
VAR_BAD_RECORD_PATH = "s3://dq-results-store/Output/bad_records/"
VAR_GOOD_RECORD_PATH = "s3://dq-results-store/Output/good_records/"

# Directory path containing JSON files
METADATA_PATH = "metadata"


# Required table metadata
REQUIRED_METADATA_FILES = {
    "dq_entity_master": "dq_entity_master.json",
    "dq_rule_master": "dq_rule_master.json",
    "dq_execution_plan": "dq_execution_plan.json"
}

# validation to be performed
DATAYPE_VALIDATION = "Column data type validation"
NULLABLE_VALIDATION = "Nullable constraint validation"
PRIMARY_KEY_UNIQUENESS_VALIDATION = "Primary key uniqueness validation"

# DataFrames dictionary to store table data
table_dataframes  = {
    "dq_entity_master": None,
    "dq_rule_master": None,
    "dq_execution_plan": None
}
