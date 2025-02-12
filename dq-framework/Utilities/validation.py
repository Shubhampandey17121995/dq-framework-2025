# Databricks notebook source
import json
import sys
import os
from pathlib import Path
from common.validation_config import REQUIRED_TABLE_METADATA, directory_path, dfs, validation_steps, validations
from pyspark.sql.functions import *

# Load Metadata
def load_selected_metadata(directory_path):
    if not os.path.exists(directory_path):
        logger.error(f"Directory '{directory_path}' does not exist.")
        return {}
    metadata = {}
    for table_name, filename in REQUIRED_TABLE_METADATA.items():
        file_path = os.path.join(directory_path, filename)
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as file:
                    table_metadata = json.load(file)
                    metadata[table_name] = table_metadata
                    logger.info(f"Loaded metadata for '{table_name}' from '{filename}'")
            except Exception as e:
                logger.error(f"Failed to load metadata from '{filename}': {e}")
        else:
            logger.error(f"File '{filename}' not found in directory '{directory_path}'")
    return metadata
#****************************************************************************************************************

# Fetch Metadata_Property

def fetch_metadata_property(metadata, table_name, property_name):
    try:
        if table_name in metadata:
            columns = metadata[table_name].get('columns', {})

            if property_name == "foreign_key":
                result = {
                    column: properties.get("reference")
                    for column, properties in columns.items()
                    if properties.get("foreign_key") and "reference" in properties
                }
                if not result:
                    logger.error(f"Foreign key does not exist for table '{table_name}'.")
            else:
                result = {
                    column: properties.get(property_name)
                    for column, properties in columns.items()
                    if property_name in properties
                }
            logger.info(f"Fetched property '{property_name}' for table '{table_name}'.")
            return result if result else {}
        else:
            logger.error(f"Table '{table_name}' not found in metadata.")
            return {}
    except Exception as e:
        logger.info(f"Error fetching '{property_name}' for table '{table_name}': {e}")
        return {}
#*********************************************************************************************************************

# Empty_df validation

def check_empty_dataframe(df,table_name):
    if df.count() == 0:
        logger.error(f"check_empty_dataframe validation failed: DataFrame for table  {table_name} is empty for entity ID {entity_id}.Please make correct entries in table {table_name} for entity_id {entity_id}")
        return False
    logger.info(f"check_empty_dataframe validation passed: No empty dataframe violations for table{table_name} for entity ID {entity_id}.")
    return True
    
#*************************************************************************************************************************

# Data_type validation
def validate_column_data_types(df, metadata,table_name):
    schema_datatype =  fetch_metadata_property(metadata, table_name,"type")
    if not schema_datatype:
        logger.info(f"validate_column_data_types validation failed: No schema_datatypes exist in table metadata for table {table_name}.Please ensure schema_datatypes exist in the table metadata for table {table_name}.")        
        return False
    df_dtypes = dict(df.dtypes)
    validation_passed = True
    for column, expected_type in schema_datatype.items():
        if column in df_dtypes:
            actual_type = df_dtypes[column]
            if actual_type != expected_type:
                logger.error(f"validate_column_data_types validation failed: Column '{column}' has expected type '{expected_type}', but found '{actual_type}'. Please make correct entries with the correct datatype for column {column} in table {table_name} for entity ID {entity_id}.")
                validation_passed = False
        else:
            logger.error(f"validate_column_data_types validation failed: Column '{column}' in table {table_name} is missing in the DataFrame for entity ID {entity_id}. Please make correct entries for column {column} in table {table_name}.")
            validation_passed = False
    if validation_passed:
        logger.info(f"validate_column_data_types validation passed: All columns in table {table_name} for entity ID {entity_id} have correct data types.")
    return validation_passed

#*****************************************************************************************************************************

# Nullable Validation

def validate_nullable_constraint(df, metadata,table_name):
    column_constraints = fetch_metadata_property(metadata, table_name,"nullable")
    if not column_constraints:
        logger.info(f"validate_nullable_constraint validation failed: No column_constraints  exist in table metadata for table {table_name}.Please ensure column_constraints exist in the table metadata for table {table_name}.")
        return False
    validation_passed = True
    for column, is_nullable in column_constraints.items():
        if is_nullable == False:
            null_count = df.filter((col(column).isNull()) | (trim(col(column)) == "")).count()
            if null_count > 0:
                logger.error(f"validate_nullable_constraint validation failed: Column '{column}' in table {table_name} contains {null_count} NULL values. Please make correct entries for column {column} in table {table_name} for entity ID {entity_id}.")
                validation_passed = False
    if validation_passed:
        logger.info(f"validate_nullable_constraint validation passed: No NULL constraint violations for table {table_name} for entity ID {entity_id}.")
        return validation_passed

#*******************************************************************************************************************************

# Primary key validation

def validate_primary_key_uniqueness(df, metadata, table_name):
    primary_keys = [key for key, value in fetch_metadata_property(metadata, table_name, "primary_key").items() if value]
    
    if not primary_keys:
        logger.info(f"Validation failed: No primary key column exists in table metadata for table {table_name}. Please ensure a primary key column exists.")
        return False

    column_name = primary_keys[0]
    total_count = df.select(df[column_name]).count()

    if total_count == 1:
        logger.info(f"Validation passed: Only {total_count} record(s) found for primary key column '{column_name}' in table {table_name}.")
        return True

    duplicate_keys_df = (df.groupBy(column_name).count().filter("count > 1").select(column_name))

    duplicate_keys = [row[column_name] for row in duplicate_keys_df.collect()]

    if duplicate_keys:
        logger.error(f"Validation failed: Duplicate values found in primary key column '{column_name}' in table {table_name}. Duplicate values: {duplicate_keys}")
        return False

    logger.info(f"Validation passed: Primary key uniqueness verified for table {table_name}.")
    return True



#*********************************************************************************************************************************

# Foreign_key validations

def validate_foreign_key_relationship(df, metadata, table_name, dfs):
    foreign_keys = fetch_metadata_property(metadata, table_name, "foreign_key")    
    if not foreign_keys:
        logger.info(f"No foreign key relationships defined for table '{table_name}'. Skipping validation")
        return True

    for child_column, reference in foreign_keys.items():
        parent_table = reference.get("table")
        parent_column = reference.get("column")

        if parent_table not in dfs:
            logger.error(f"Parent table '{parent_table}' not found in provided DataFrames.")
            return False

        parent_df = dfs[parent_table]

        missing_rows = df.join(parent_df, df[child_column] == parent_df[parent_column], "left_anti")
        missing_count = missing_rows.count()

        if missing_count > 0:
            missing_values = missing_rows.select(child_column).rdd.flatMap(lambda x: x).collect()
            logger.error(
                f"Validation failed: {missing_count} missing foreign key values in '{table_name}.{child_column}' referencing '{parent_table}.{parent_column}'. Missing values: {missing_values}"
            )
            return False

    logger.info(f"Validation passed: Foreign key relationships verified for table '{table_name}'.")
    return True
#************************************************************************************************************************************

# Apply validations

def apply_validation(filter_df, metadata, table_name):
    validation_passed = True
    if not check_empty_dataframe(filter_df, table_name):
        logger.error("Validation failed: DataFrame is empty. Skipping further validations.")
        return False
    for validation_name, validation_func in validation_steps:
        try:
            result = validation_func(filter_df, metadata, table_name)
            if not result:
                logger.error(f"{validation_name} failed for table {table_name}.")
                validation_passed = False
        except Exception as e:
            logger.error(f"Exception during {validation_name} for table {table_name}: {e}")
            validation_passed = False
    if validation_passed:
        logger.info(f"Validation process passed for table {table_name}.")
    else:
        logger.info(f"Validation process failed for table {table_name}.")
    return validation_passed
#**********************************************************************************************************************************

# Execute Validations

def execute_validations(validations):
    validation_passed = True
    for validation_func, args in validations:
        if not validation_func(*args):
            logger.error("Metadata validation failed! Process further validations.")
            validation_passed = False
            
    if not validation_passed:
        logger.error("Some metadata validations failed. Please check the logs for details.\n Hence Metadata Validation process failed")
        return validation_passed
    logger.info("Metadata validation process completed successfully.")
    return validation_passed
#********************************************************************************************************************************

