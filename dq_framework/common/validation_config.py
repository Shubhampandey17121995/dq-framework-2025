
import json
import os
from pathlib import Path
from pyspark.sql.functions import *
from Utilities.validation import *
from common.constants import DIRECTORY_PATH, REQUIRED_TABLE_METADATA

# Dictionary with table_names with their respective dfs
dfs = {
    "dq_entity_master": entity_master_filtered_df,
    "df_rule_master": rule_master_filtered_df,
    "dq_execution_plan": execution_plan_filtered_df
}

# Loads metadata JSON files from the specified directory path.
# Returns a dictionary with table names as keys and their metadata as values.
def load_selected_metadata(directory_path):
    metadata = {}
    if not os.path.exists(directory_path):
        logger.error(f"Directory '{directory_path}' does not exist.")
        return metadata
    
    for table_name, filename in REQUIRED_TABLE_METADATA.items():
        file_path = os.path.join(directory_path, filename)
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as file:
                    metadata[table_name] = json.load(file)
                    logger.info(f"Loaded metadata for '{table_name}' from '{filename}'")
            except Exception as e:
                logger.error(f"Failed to load metadata from '{filename}': {e}")
        else:
            logger.error(f"File '{filename}' not found in directory '{directory_path}'")
    
    return metadata

metadata = load_selected_metadata(DIRECTORY_PATH)

# Generate validation list dynamically
validations = [(apply_validation, (df, metadata, table_name)) for table_name, df in dfs.items()]
validations.extend((validate_foreign_key_relationship, (df, metadata, table_name, dfs)) for table_name, df in dfs.items())