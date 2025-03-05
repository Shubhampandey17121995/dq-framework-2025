# Databricks notebook source
import json
from pyspark.sql.functions import col,trim,count
from pyspark.sql import DataFrame 
from common.constants import *
from common.custom_logger import *
import importlib.resources as pkg_resources
#logger = getlogger()
import logging
logger = get_logger()

# Load required metadata for tables from JSON files
def load_metadata():
    """
    Loads required metadata for tables from JSON files defined in REQUIRED_TABLE_METADATA.

    Iterates through each table and its associated metadata file, reads the JSON content,
    extracts column information, and handles potential errors like missing files or invalid JSON.

    Returns:
        dict: A dictionary where keys are table names and values are lists of column names.
              If metadata is missing or an error occurs, an empty list is assigned to the table.
    """
    
    # Dictionary to store metadata for each table
    required_metadata = {}
    # Iterate through each table and its corresponding metadata file
    for table_name, filename in REQUIRED_METADATA_FILES.items():
        try:
            # Construct the file path using pkg_resources
            metadata_path = pkg_resources.files(METADATA_PATH).joinpath(filename)
            # Open and load JSON metadata content
            with metadata_path.open('r', encoding='utf-8') as f:
                metadata = json.load(f)
            # Directly get columns, default to empty if not found
            required_metadata[table_name] = metadata.get(table_name, {}).get("columns", [])
            logger.info(f"[DQ_LOAD_METADATA] Metadata loaded successfully for table '{table_name}' from '{filename}'. STATUS:'SUCCESS'.")
        # Handle file not found, invalid JSON, or key errors
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            logger.error(f"[DQ_LOAD_METADATA] Error loading '{filename}' for table '{table_name}': {e}. STATUS:'FAILED'.")
            required_metadata[table_name] = []
        # Handle unexpected errors gracefully
        except Exception as e:
            logger.error(f"[DQ_LOAD_METADATA] Unexpected error while loading '{filename}' for table '{table_name}': {e}. STATUS:'FAILED'.")
            required_metadata[table_name] = []
    # Return the compiled metadata dictionary
    return required_metadata


# Fetch specific metadata property for a given table
def extract_metadata_property(metadata, table_name, property_name):
    """
    Retrieves a specific metadata property for a given table from the provided metadata.

    This function searches for a specific property (e.g., data type, foreign key) within the metadata
    of a given table. If the property is "foreign_key", it fetches the "references" key instead.

    Args:
        metadata (dict): The complete metadata dictionary containing table details.
        table_name (str): The name of the table for which the metadata is to be fetched.
        property_name (str): The specific property to fetch (e.g., "data_type", "foreign_key").

    Returns:
        dict: A dictionary mapping column names to their respective property values.
            Returns an empty dictionary if the table or property is not found.
    """
    try:
        # Ensure the table exists in metadata
        if table_name not in metadata:
            logger.warning(f"[DQ_EXTRACT_METADATA_PROPERTY] Table '{table_name}' not found in metadata. STATUS:'FAILED'.")
            return {}
        # Retrieve the columns for the specified table
        columns = metadata[table_name]
        if not columns:
            logger.warning(f"[DQ_EXTRACT_METADATA_PROPERTY] No columns found in metadata for table '{table_name}'. STATUS:'FAILED'.")
            return {}
        # Process and extract the requested property
        if property_name == "foreign_key":
            result = {
                col["name"]: col.get("references")
                for col in columns
                if col.get("foreign_key") and "references" in col
            }
        else:
            result = {
                col["name"]: col.get(property_name)
                for col in columns
                if property_name in col
            }
        # Log success and return the extracted metadata
        logger.info(f"[DQ_EXTRACT_METADATA_PROPERTY] Fetched property '{property_name}' for table '{table_name}'. STATUS:'SUCCESS'.")
        return result if result else {}
    except Exception as e:
        logger.error(f"[DQ_EXTRACT_METADATA_PROPERTY] Exception occured in fetch_metadata_property : Error extracting '{property_name}' for '{table_name}': {e}. STATUS:'FAILED'.")
        return {}


# Check if DataFrame is empty
def is_df_empty(df, table_name, entity_id):
    """
    Validates whether the provided  DataFrame is empty.

    This function checks if the given DataFrame is valid and contains data. It logs appropriate 
    messages based on the validation result and handles potential errors gracefully.

    Args:
        df (DataFrame): The Spark DataFrame to validate.
        table_name (str): The name of the table associated with the DataFrame (used for logging).
        entity_id (int): The entity identifier, for tracking/logging purposes.

    Returns:
        bool: 
            - True if the DataFrame is valid and non-empty.
            - False if the DataFrame is invalid, empty, or an exception occurs.
    """
    try:
        # Validate that the input is a Spark DataFrame
        if not isinstance(df, DataFrame):
            logger.error(f"[DQ_VALIDATION] check_empty_dataframe validation failed! Invalid object type for table '{table_name}'. Expected DataFrame, got {type(df)}. STATUS:'FAILED'.")
            return False
        # Check if the DataFrame is empty using rdd.isEmpty() for efficiency
        if df.limit(1).count() == 0:
            logger.error(f"[DQ_VALIDATION] check_empty_dataframe validation failed! DataFrame for table '{table_name}' is empty for entity_id '{entity_id}'. STATUS:'FAILED'.")
            return False
        # Log success if DataFrame is valid and non-empty
        logger.info(f"[DQ_VALIDATION] check_empty_dataframe validation passed! DataFrame for table '{table_name}' is not empty for entity_id '{entity_id}'. STATUS:'SUCCESS'.")
        return True
    except Exception as e:
        # Log unexpected errors and return False
        logger.error(f"[DQ_VALIDATION] Exception in check_empty_dataframe for table '{table_name}', entity_id '{entity_id}': {e}. STATUS: 'FAILED'.")
        return False


# Validate DataFrame column data types
def validate_column_types(df, metadata, table_name, entity_id):
    """
    Validates that the data types of DataFrame columns match the expected types from metadata.

    This function compares each column's data type in the DataFrame against the expected data type
    defined in the metadata. It logs mismatches and returns the validation status.

    Args:
        df (DataFrame): The Spark DataFrame to validate.
        metadata (dict): Metadata containing expected data types for table columns.
        table_name (str): The name of the table for logging purposes.
        entity_id (int): The entity identifier for contextual logging.

    Returns:
        bool: 
            - True if all columns match their expected data types.
            - False if any mismatch is found or if an error occurs.
    """
    try:
        # Fetch expected data types from metadata for the given table
        expected_schema = extract_metadata_property(metadata, table_name, "type")
        if not expected_schema:
            logger.error(f"[DQ_VALIDATION] No schema data type found in metadata for table '{table_name}'. STATUS:'FAILED'.")
            return False
        # Get actual data types from the DataFrame
        df_dtypes = dict(df.dtypes)
        validation_passed = True
        # Compare each column's actual data type with the expected type
        for column, expected_type in expected_schema.items():
            actual_type = df_dtypes.get(column)
            if actual_type != expected_type:
                logger.error(f"[DQ_VALIDATION] validate_column_types validation failed: Column '{column}' in '{table_name}' expected '{expected_type}', found '{actual_type}' for (entity_id: {entity_id}). STATUS:'FAILED'.")
                validation_passed = False
        # Log success if all columns have the correct data types
        if validation_passed:
            logger.info(f"[DQ_VALIDATION] validate_column_types validation passed:All columns in table '{table_name}' have correct data types for entity_id '{entity_id}'. STATUS:'SUCCESS'.")
        return validation_passed
    except Exception as e:
        # Log unexpected errors and return False
        logger.error(f"[DQ_VALIDATION] Exception in validate_column_data_types for table '{table_name}', entity_id '{entity_id}': {e}. STATUS:'FAILED'.")
        return False


# Validate nullable constraints
def validate_nullable_constraints(df, metadata, table_name, entity_id):
    """
    Validates that non-nullable columns in the DataFrame do not contain NULL or empty values.

    This function checks each column defined as non-nullable in the metadata and verifies that 
    the DataFrame does not contain any NULL or empty string values in these columns.

    Args:
        df (DataFrame): The Spark DataFrame to validate.
        metadata (dict): Metadata containing nullable constraints for table columns.
        table_name (str): The name of the table for logging purposes.
        entity_id (int): The entity identifier for contextual logging.

    Returns:
        bool: 
            - True if all non-nullable columns have no NULL or empty values.
            - False if any constraint violation is found or if an error occurs.
    """
    try:
        # Fetch nullable constraints from metadata for the given table
        nullable_constraints = extract_metadata_property(metadata, table_name, "nullable")
        if not nullable_constraints:
            logger.error(f"[DQ_VALIDATION] No nullable constraint found for table '{table_name}'. STATUS:'FAILED'.")
            return False
        validation_passed = True
        # Iterate over each column to check for NULL or empty values in non-nullable columns
        for column, is_nullable in nullable_constraints.items():
            if not is_nullable:
                # Count NULL or empty string values in the non-nullable column
                null_count = df.filter((col(column).isNull()) | (trim(col(column)) == "")).count()
                if null_count > 0:
                    logger.error(f"[DQ_VALIDATION] Column '{column}' in table '{table_name}' has {null_count} NULL  values for entity_id '{entity_id}'. STATUS:'FAILED'.")
                    validation_passed = False
        # Log success if no violations are found
        if validation_passed:
            logger.info(f"[DQ_VALIDATION] No NULL constraint violations in table '{table_name}'for entity_id '{entity_id}'. STATUS:'SUCCESS'.")
        return validation_passed
    except Exception as e:
        # Log unexpected errors and return False
        logger.error(f"[DQ_VALIDATION] Exception in validate_nullable_constraint for table '{table_name}', entity_id '{entity_id}': {e}. STATUS:'FAILED'.")
        return False

# Validate primary key uniqueness
def validate_primary_key_uniqueness(df, metadata, table_name, entity_id):
    """
    Validates the uniqueness of the primary key column in the given DataFrame.

    This function checks if the primary key defined in the metadata has unique values 
    across all rows in the DataFrame. It also handles cases where no primary key is defined 
    or only a single record exists.

    Args:
        df (DataFrame): The Spark DataFrame to validate.
        metadata (dict): Metadata containing primary key information.
        table_name (str): The name of the table for logging purposes.
        entity_id (int): The entity identifier for contextual logging.

    Returns:
        bool: 
            - True if the primary key is unique or if only one record exists.
            - False if duplicate primary keys are found or in case of errors.
    """
    try:
        # Fetch primary key information from metadata
        primary_key_columns = [key for key, value in extract_metadata_property(metadata, table_name, "primary_key").items() if value]
        # Validate that primary key exists in metadata
        if not primary_key_columns:
            logger.error(f"[DQ_VALIDATION] No primary key defined in metadata for table '{table_name}'. STATUS:'FAILED'.")
            return False
        # Assuming single primary key column for validation
        column_name = primary_key_columns[0]
        # Count total records in the DataFrame
        total_count = df.select(col(column_name)).count()
        # If only one record exists, uniqueness is implicitly valid
        if total_count == 1:
            logger.info(f"[DQ_VALIDATION] Only one record found for primary key column '{column_name}' in table '{table_name}' for entity id {entity_id}.STATUS:'SUCCESS'.")
            return True
        # Identify duplicate primary keys by grouping and filtering counts > 1
        duplicate_keys_df = df.groupBy(column_name).agg(count("*").alias("count")).filter(col("count") > 1)       
        duplicate_keys = [row[column_name] for row in duplicate_keys_df.collect()]
        # Log error if duplicates exist
        if duplicate_keys:
            logger.error(f"[DQ_VALIDATION] Duplicate values found in primary key column '{column_name}' in table '{table_name}': {duplicate_keys} for entity_id '{entity_id}'. STATUS:'FAILED'.")
            return False
        # Log success if primary key uniqueness is validated
        logger.info(f"[DQ_VALIDATION] Primary key uniqueness verified for table '{table_name}' for entity_id '{entity_id}'. STATUS:'SUCCESS'.")
        return True
    except Exception as e:
        # Log unexpected errors and return False
        logger.error(f"[DQ_VALIDATION] Exception in validate_primary_key_uniqueness for table '{table_name}', entity_id '{entity_id}': {e}. STATUS:'FAILED'.")
        return False


# Validate foreign key relationships
def validate_foreign_key_constraints(df, metadata, table_name, table_dataframes, entity_id):
    """
    Validates foreign key relationships for the given DataFrame against parent tables.

    This function ensures that the foreign key values in the child table (df) 
    have corresponding values in the referenced parent tables, maintaining referential integrity.

    Args:
        df (DataFrame): The Spark DataFrame (child table) to validate.
        metadata (dict): Metadata containing foreign key relationship details.
        table_name (str): The name of the table being validated.
        dfs (dict): A dictionary of DataFrames for all tables, with table names as keys.
        entity_id (int): The entity identifier for logging purposes.

    Returns:
        bool:
            - True if all foreign key relationships are valid or if no foreign keys are defined.
            - False if there are missing references or in case of errors.
    """
    try:
        # Retrieve foreign key constraints from metadata
        foreign_keys = extract_metadata_property(metadata, table_name, "foreign_key")
        # If no foreign keys exist, log and return True (no validation needed)
        if not foreign_keys:
            logger.info(f"[DQ_VALIDATION] Skipping foreign key validation: No foreign keys defined for '{table_name}' (entity_id: {entity_id}). STATUS:'SUCCESS'.")
            return True
        # Iterate through each foreign key relationship
        for child_column, reference in foreign_keys.items():
            parent_table = reference.get("table")
            parent_column = reference.get("column")
            # Check if the parent table exists in the provided DataFrames dictionary
            if parent_table not in table_dataframes:
                logger.error(f"[DQ_VALIDATION] validate_foreign_key_relationship validation failed: Parent table '{parent_table}' not found in provided DataFrames for entity_id '{entity_id}'. STATUS:'FAILED'.")
                return False
            parent_df = table_dataframes[parent_table]
            # Perform a left anti-join to find foreign key values in the child table that do not exist in the parent table
            invalid_fk_rows = df.join(parent_df, df[child_column] == parent_df[parent_column], "left_anti")
            missing_count = invalid_fk_rows.count()
            # If missing values are found, log an error and return False
            if missing_count > 0:
                missing_values = [row[child_column] for row in invalid_fk_rows.collect()]
                logger.error(f"[DQ_VALIDATION] validate_foreign_key_relationship validation failed: {missing_count} missing foreign key values in '{table_name}.{child_column}'. STATUS:'FAILED'."
                             f"referencing '{parent_table}.{parent_column}' for entity_id '{entity_id}'. "
                             f"Missing values: {missing_values}")
                return False
        # If all foreign key relationships are valid, log a success message
        logger.info(f"[DQ_VALIDATION] validate_foreign_key_relationship validation passed: All foreign key relationships verified for table '{table_name}' for entity_id '{entity_id}'. STATUS:'SUCCESS'.")
        return True
    except Exception as e:
        # Log unexpected errors and return False
        logger.error(f"[DQ_VALIDATION] Exception occurred during foreign key validation for table '{table_name}',entity_id '{entity_id}': {str(e)}. STATUS:'FAILED'.")
        return False


# Validation Configuration  required for apply_validation
VALIDATION_STEPS = [
    (DATAYPE_VALIDATION, validate_column_types),
    (NULLABLE_VALIDATION, validate_nullable_constraints),
    (PRIMARY_KEY_UNIQUENESS_VALIDATION, validate_primary_key_uniqueness)
]


# Applies a series of validation checks to the given DataFrame based on predefined validation steps.
def apply_validation(filter_df, metadata, table_name, entity_id):
    try:
        # Initialize validation flag as True
        validation_passed = True    
        logger.info(f"Beginning  Validation Process for table '{table_name}'")    
        # Check if the DataFrame is empty, if not then only go for further validations
        if not is_df_empty(filter_df, table_name, entity_id):
            return False        
        # Iterate through each validation function and apply it
        for validation_name, validation_func in VALIDATION_STEPS:
            try:
                result = validation_func(filter_df, metadata, table_name, entity_id)
                if not result:
                    logger.error(f"[DQ_VALIDATION] {validation_name} failed for table {table_name} for entity_id '{entity_id}'. STATUS:'FAILED'.")
                    validation_passed = False
            except Exception as e:
                logger.error(f"[DQ_VALIDATION] Exception during {validation_name} for table {table_name}: {e} for entity_id '{entity_id}'. STATUS:'FAILED'.")
                validation_passed = False        
        # Log the overall validation result
        if validation_passed:
            logger.info(f"[DQ_VALIDATION] Metadata validation process passed for table {table_name} for entity_id '{entity_id}'. STATUS:'SUCCESS'.")
        else:
            logger.info(f"[DQ_VALIDATION] Metadata validation process failed for table {table_name} for entity_id '{entity_id}'. STATUS:'FAILED'.")        
        return validation_passed   
    # Return False in case of any unexpected exception   
    except Exception as e:
        logger.error(f"[DQ_VALIDATION] Unexpected error occurred during validation process for table {table_name} for entity_id '{entity_id}': {str(e)}. STATUS:'FAILED'.")
        return False

# Generate dynamic validation list for each dtaframes to pass execute_validations function
def generate_validation(TABLE_DATAFRAMES,metadata,entity_id):
    validations = [(apply_validation, (df, metadata, table_name,entity_id)) for table_name, df in table_dataframes.items()]
    validations.extend((validate_foreign_key_constraints, (df, metadata, table_name, table_dataframes,entity_id)) for table_name, df in table_dataframes.items())
    return validations

# Executes metadata validation functions sequentially and logs validation results.
def execute_validations(validations):
    try:
        # Initialize validation flag as True
        validation_passed = True        
        # Iterate through each validation function and execute it
        for validation_func, args in validations:
            try:
                if not validation_func(*args):                    
                    validation_passed = False
            except Exception as e:
                logger.error(f"[DQ_VALIDATION] Exception during metadata validation: {str(e)}. STATUS:'FAILED'.")
                validation_passed = False        
        # If any validation failed, log final error message and return False        
        if not validation_passed:
            logger.error("[DQ_VALIDATION] Some metadata validations failed. Please check the logs for details.\n Hence Metadata Validation process failed. STATUS:'FAILED'.")
            return validation_passed        
        # Log success message if all validations passed
        logger.info("[DQ_VALIDATION] Metadata validation process completed successfully. STATUS:'SUCCESS'.")
        return validation_passed    
    # Return False in case of any unexpected exception
    except Exception as e:
        logger.error(f"[DQ_VALIDATION] Unexpected error occurred during metadata validation execution: {str(e)}. STATUS:'FAILED'.")
        return False