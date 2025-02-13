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

# entity id
VAR_ENTITY_ID = None

# config table paths
VAR_S3_RULE_MASTER_PATH = "job_catalog.dq_testdb2.dq_entity_master"
VAR_S3_ENTITY_MASTER_PATH = "job_catalog.dq_testdb2.dq_execution_plan"
VAR_S3_EXECUTION_PLAN_PATH = "job_catalog.dq_testdb2.dq_execution_result"
VAR_S3_EXECUTION_RESULT_PATH = "job_catalog.dq_testdb2.df_rule_master"

# result store paths
VAR_BAD_RECORD_PATH = "s3://dq-results-store/bad_records/"
VAR_GOOD_RECORD_PATH = "s3://dq-results-store/good_records/"

# Directory path containing JSON files
DIRECTORY_PATH = "dq-framework/metadata"

# Required table metadata
REQUIRED_TABLE_METADATA = {
    "dq_entity_master": "dq_entity_master.json",
    "df_rule_master": "dq_rule_master.json",
    "dq_execution_plan": "dq_execution_plan.json"
}


# Validation Configuration  required for apply_validation
VALIDATION_STEPS = [
    ("Column data type validation", "validate_column_data_types"),
    ("Nullable constraint validation", "validate_nullable_constraint"),
    ("Primary key uniqueness validation", "validate_primary_key_uniqueness")
]
