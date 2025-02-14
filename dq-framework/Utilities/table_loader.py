# Load configuration table data
def config_loader(df, entity_id):
    try:
        # Filter the DataFrame for the specific entity_id
        filtered_df = df.filter(col("entity_id") == entity_id)
        
        # Check if DataFrame is empty
        if filtered_df.rdd.isEmpty():
            logger.warning(f"No records found for entity_id: {entity_id}")
            return None
        
        logger.info(f"Successfully loaded {filtered_df.count()} records for entity_id: {entity_id}")
        return filtered_df

    except Exception as e:
        logger.error(f"Error filtering DataFrame for entity_id: {entity_id} - {e}")
        return None

# load actual entity data
def entity_data_loader(spark, entity_master_path, execution_plan_path, execution_result_path, rule_master_path):
        entity_master_df = spark.read.format("iceberg").table(entity_master_path)
        execution_plan_df = spark.read.format("iceberg").table(execution_plan_path)
        execution_result_df = spark.read.format("iceberg").table(execution_result_path)
        rule_master_df = spark.read.format("iceberg").table(rule_master_path)
          
        return entity_master_df, execution_plan_df, execution_result_df, rule_master_df



def data_loader(entity_path):
        # Extract file extension
        file_format = entity_path.split('.')[-1].lower()

        # Read the file dynamically using file extension
        try:
                logger.info(f"Attempting to read file: {entity_path} as {file_format.upper()}")
                df = spark.read.format(file_format).option("header", "true").option("inferSchema", "true").load(entity_path)
                logger.info(f"Successfully read file as {file_format.upper()}")
        except Exception as e:
                logger.error(f"Error reading file {entity_path}: {e}\nUnsupported or unknown file format.")


