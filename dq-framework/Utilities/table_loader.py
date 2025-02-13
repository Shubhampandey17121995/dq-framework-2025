# Load configuration table data
def config_loader(df,entity_id):
        """
        load the dataframe from path and filter data for given entity id
        args:df,entity_id
                1. read the dataframe of the given file_path or table name
                2. from dataframe filter records related to given entity id
        return dataframe : the dataframe with the data from given entity id
        """
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
def entity_data_loader(entity_master_path: str, execution_plan_path: str, execution_result_path: str, rule_master_path: str):
        """
        This function is to load the actual data of an entity.
        This loads the dataframe from given file_path or table_name.
        s3_table_path : file_path or table_name of the data table
                1. read the dataframe of the given file_path or table name
        return dataframe : dataframe with data of the entity
        """
        entity_master_df = spark.read.format("iceberg").table(entity_master_path)
        execution_plan_df = spark.read.format("iceberg").table(execution_plan_path)
        execution_result_df = spark.read.format("iceberg").table(execution_result_path)
        rule_master_df = spark.read.format("iceberg").table(rule_master_path)
    
        return entity_master_df, execution_plan_df, execution_result_df, rule_master_df
