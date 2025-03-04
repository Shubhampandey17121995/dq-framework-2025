from pyspark.sql.functions import col
from common.custom_logger import getlogger
logger = getlogger()

# Load configuration table data
def filter_config_by_entity(df, entity_id):
    try:
        # Filter the DataFrame for the specific entity_id
        filtered_df = df.filter(col("entity_id") == entity_id)
       
        # Check if DataFrame is empty
        if filtered_df.rdd.isEmpty():
            logger.warning(f"[CONFIG LOADER] No records found for entity_id: {entity_id}")
            return None
       
        logger.info(f"[CONFIG LOADER] Successfully loaded {filtered_df.count()} records for entity_id: {entity_id}")
        return filtered_df
 
    except Exception as e:
        logger.error(f"[CONFIG LOADER] Error filtering DataFrame for entity_id: {entity_id} - {e}")
        return None
 
# load actual entity data
def fetch_tables(spark, entity_master_path, execution_plan_path, execution_result_path, rule_master_path):
    try:
        logger.info("Starting entity data loading process...")
 
        entity_master_df = spark.read.format("iceberg").table(entity_master_path)
        execution_plan_df = spark.read.format("iceberg").table(execution_plan_path)
        execution_result_df = spark.read.format("iceberg").table(execution_result_path)
        rule_master_df = spark.read.format("iceberg").table(rule_master_path)
 
        logger.info(f"[FETCH_TABLE] Successfully loaded entity_master table: {entity_master_path}, records: {entity_master_df.count()}")
        logger.info(f"[FETCH_TABLE] Successfully loaded execution_plan table: {execution_plan_path}, records: {execution_plan_df.count()}")
        logger.info(f"[FETCH_TABLE] Successfully loaded execution_result table: {execution_result_path}, records: {execution_result_df.count()}")
        logger.info(f"[FETCH_TABLE] Successfully loaded rule_master table: {rule_master_path}, records: {rule_master_df.count()}")
 
        return entity_master_df, execution_plan_df, execution_result_df, rule_master_df
 
    except Exception as e:
        logger.error(f" Error loading entity data: {e}")
        return None, None, None, None
 
 
 
def load_entity_data(spark,entity_path):
        # Extract file extension
        file_format = entity_path.split('.')[-1].lower()
 
        # Read the file dynamically using file extension
        try:
                logger.info(f"[ENTITY DATA LOADER] Attempting to read file: {entity_path} as {file_format.upper()}")
                df = spark.read.format(file_format).option("header", "true").option("inferSchema", "true").load(entity_path)
                logger.info(f"[ENTITY DATA LOADER] Successfully read file as {file_format.upper()}")
        except Exception as e:
                logger.error(f"[ENTITY DATA LOADER] Error reading file {entity_path}: {e}\nUnsupported or unknown file format.")