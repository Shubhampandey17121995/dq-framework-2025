# Databricks notebook source
import json
from pyspark.sql.functions import col,trim
from pyspark.sql import DataFrame
import common.constants
from common.constants import DIRECTORY_PATH, REQUIRED_TABLE_METADATA
from common.custom_logger import getlogger
import importlib.resources as pkg_resources
import importlib
logger = getlogger()


# Load Required Metadata
def load_required_metadata():
    """
    Loads required metadata for tables from JSON files.

    Returns:
        dict: A dictionary containing table names as keys and their respective column metadata as values.
    """
    required_metadata = {}

    # Iterate through the required metadata dictionary
    for table_name, filename in REQUIRED_TABLE_METADATA.items():
        try:
            # Open and read the metadata file
            with pkg_resources.files(DIRECTORY_PATH).joinpath(filename).open('r', encoding='utf-8') as f:
                metadata_content = json.load(f)
                # Check if metadata contains the table name and extract columns
                if table_name in metadata_content:
                    table_metadata = metadata_content[table_name]
                    if "columns" in table_metadata:
                        required_metadata[table_name] = table_metadata["columns"]
                        logger.info(f"Loaded metadata for '{table_name}' from '{filename}'")
                    else:
                        logger.error(f"Table '{table_name}' does not contain a 'columns' key.")
                else:
                    logger.error(f"Table '{table_name}' not found in metadata file '{filename}'")
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            # Assign an empty list if metadata loading fails to prevent runtime errors
            required_metadata[table_name] = []  
    return required_metadata

   
   

# Fetch a specific property from the metadata for a given table
def fetch_metadata_property(metadata, table_name, property_name):
    """
    Retrieves a specific property from the metadata for a given table.

    Args:
        metadata (dict): The metadata dictionary containing table details.
        table_name (str): The name of the table to fetch metadata for.
        property_name (str): The specific property to retrieve (e.g., "nullable","type,"primary_key","foreign_key").

    Returns:
        dict: A dictionary mapping column names to the requested property values.
              Returns an empty dictionary if the table or property is not found.
    """
    try:
        # Ensure the table exists in metadata
        if table_name not in metadata:
            logger.error(f"Table '{table_name}' not found in metadata.")
            return {}
        # Retrieve the columns for the specified table
        columns = metadata[table_name]
        if not columns:
            logger.info(f"No columns found in metadata for table '{table_name}'.")
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
        logger.info(f"Fetched property '{property_name}' for table '{table_name}'.")
        return result if result else {}
    except Exception as e:
        logger.error(f"Error fetching '{property_name}' for table '{table_name}': {e}")
        return {}

       
      
   
# Validate whether the given DataFrame is empty or not
def check_empty_dataframe(df, table_name, entity_id):
    """
    Validates whether the given DataFrame is empty.
    
    Args:
        df (DataFrame): The Spark DataFrame to validate.
        table_name (str): The name of the table associated with the DataFrame.
        entity_id (int): The entity identifier for logging purposes.
        
    Returns:
        bool: True if the DataFrame is non-empty, False otherwise.
    """
    try:
        # Ensure the provided object is a valid Spark DataFrame
        if not isinstance(df, DataFrame):
            logger.error(f"Invalid object type for table '{table_name}'. Expected a DataFrame, got {type(df)}.")
            return False  
        # Check if the DataFrame is empty
        if df.count() == 0:
            logger.error(f"check_empty_dataframe validation failed: DataFrame for table '{table_name}' is empty "
                         f"for entity_id '{entity_id}'. Ensure correct data entries.")
            return False
        # If DataFrame is valid and non-empty, log success message
        logger.info(f"check_empty_dataframe validation passed: DataFrame for table '{table_name}' is not empty "
                    f"for entity_id '{entity_id}'.")
        return True
    except Exception as e:
        # Log unexpected errors and return False
        logger.error(f"Exception occured in check_empty_dataframe for table '{table_name}', entity_id '{entity_id}': {str(e)}")
        return False 


   
  
# Validate whether the DataFrame columns have the expected data types based on metadata schema
def validate_column_data_types(df, metadata, table_name, entity_id):
    """
    Validates whether the columns in the given DataFrame match the expected data types 
    as defined in the metadata schema.
    
    Args:
        df (DataFrame): The Spark DataFrame to validate.
        metadata (dict): The metadata dictionary containing schema definitions.
        table_name (str): The name of the table being validated.
        entity_id (int): The entity identifier for logging purposes.
        
    Returns:
        bool: True if all columns have the expected data types, False otherwise.
    """
    try:
        # Fetch the expected data types from metadata
        schema_datatype = fetch_metadata_property(metadata, table_name, "type")        
        # If metadata is missing data type definitions, log an error and return False
        if not schema_datatype:
            logger.info(f"validate_column_data_types validation failed: No schema data types found in metadata "
                        f"for table '{table_name}'. Ensure schema definitions exist for entity_id '{entity_id}'.")
            return False
        # Extract actual data types from the DataFrame
        df_dtypes = dict(df.dtypes)
        validation_passed = True
        # Iterate over expected columns and validate data types
        for column, expected_type in schema_datatype.items():
            if column in df_dtypes:
                actual_type = df_dtypes[column]
                # Check if actual data type matches the expected type
                if actual_type != expected_type:
                    logger.error(f"validate_column_data_types validation failed: Column '{column}' in table '{table_name}' has expected type "
                                 f"'{expected_type}', but found '{actual_type}'. Correct data type entries for entity_id '{entity_id}'.")
                    validation_passed = False
            else:
                # Log an error if the column is missing in the DataFrame
                logger.error(f"validate_column_data_types validation failed: Column '{column}' is missing in table '{table_name}' "
                             f"for entity_id '{entity_id}'. Ensure it exists in the DataFrame.")
                validation_passed = False
        # If validation is successful, log a success message
        if validation_passed:
            logger.info(f"validate_column_data_types validation passed: All columns in table '{table_name}' for entity_id '{entity_id}' have correct data types.")
        return validation_passed
    except Exception as e:
        # Log unexpected errors and return False
        logger.error(f"Exception occured in validate_column_data_types for table '{table_name}', entity_id '{entity_id}': {str(e)}")
        return False  


   

# Validate whether the DataFrame columns adhere to the nullable constraints defined in the metadata
def validate_nullable_constraint(df, metadata, table_name, entity_id):
    """
    Validates whether the columns in the given DataFrame adhere to the nullable constraints 
    as defined in the metadata schema.
    
    Args:
        df (DataFrame): The Spark DataFrame to validate.
        metadata (dict): The metadata dictionary containing nullable constraints.
        table_name (str): The name of the table being validated.
        entity_id (int): The entity identifier for logging purposes.
        
    Returns:
        bool: True if all nullable constraints are satisfied, False otherwise.
    """
    try:
        # Fetch nullable constraints from metadata
        column_constraints = fetch_metadata_property(metadata, table_name, "nullable")
        # If constraints are missing, log an error and return False
        if not column_constraints:
            logger.info(f"validate_nullable_constraint validation failed: No nullable constraints found in metadata for table '{table_name}'. Ensure constraints are defined for entity_id '{entity_id}'.")
            return False
        # Initialize validation flag
        validation_passed = True
        # Iterate over columns and validate nullable constraints
        for column, is_nullable in column_constraints.items():
            if not is_nullable:  # If column should NOT be nullable
                try:
                    # Count NULL or empty string values in the column
                    null_count = df.filter((col(column).isNull()) | (trim(col(column)) == "")).count()
                    # Log an error if NULL values exist
                    if null_count > 0:
                        logger.error(f"validate_nullable_constraint validation failed: Column '{column}' in table '{table_name}'contains {null_count} NULL values. Ensure correct entries for entity_id '{entity_id}'.")
                        validation_passed = False
                except Exception as e:
                    logger.error(f"Error checking NULL constraint for column '{column}' in table '{table_name}' entity_id '{entity_id}': {str(e)}")
                    validation_passed = False
        # If validation passes, log a success message
        if validation_passed:
            logger.info(f"validate_nullable_constraint validation passed: No NULL constraint violations in table '{table_name}' for entity_id '{entity_id}'.")
        return validation_passed
    except Exception as e:
        # Log unexpected errors and return False
        logger.error(f"Exception occured in validate_nullable_constraint for table '{table_name}', entity_id '{entity_id}': {str(e)}")
        return False  
       
       
# Validate whether the primary key column in the given DataFrame has unique values as per the metadata definition.
def validate_primary_key_uniqueness(df, metadata, table_name, entity_id):
    """
    Validates whether the primary key column in the given DataFrame contains unique values 
    as per the metadata definition.
    
    Args:
        df (DataFrame): The Spark DataFrame to validate.
        metadata (dict): The metadata dictionary containing primary key definitions.
        table_name (str): The name of the table being validated.
        entity_id (int): The entity identifier for logging purposes.
        
    Returns:
        bool: True if the primary key column has unique values, False otherwise.
    """
    try:
        # Retrieve primary key column(s) from metadata
        primary_keys = [key for key, value in fetch_metadata_property(metadata, table_name, "primary_key").items() if value]
        # If no primary key is defined, log an error and return False
        if not primary_keys:
            logger.info(f"validate_primary_key_uniqueness validation failed: No primary key defined in metadata for table '{table_name}'. Ensure a primary key exists for entity_id '{entity_id}'.")
            return False
        # Extract the first primary key column (assuming single-column primary key)
        column_name = primary_keys[0]
        # Count total number of records in the DataFrame
        total_count = df.select(df[column_name]).count()
        # If only one record exists, uniqueness is inherently satisfied
        if total_count == 1:
            logger.info(f"validate_primary_key_uniqueness validation passed: Only {total_count} record(s) found for primary key column '{column_name}' in table '{table_name}' for entity_id '{entity_id}'.")
            return True
        # Identify duplicate values in the primary key column
        duplicate_keys_df = df.groupBy(column_name).count().filter("count > 1").select(column_name)
        # Collect duplicate primary key values for logging
        duplicate_keys = [row[column_name] for row in duplicate_keys_df.collect()]
        # If duplicates exist, log an error and return False
        if duplicate_keys:
            logger.error(f"validate_primary_key_uniqueness validation failed: Duplicate values found in primary key column '{column_name}' in table '{table_name}' for entity_id '{entity_id}'. Duplicate values: {duplicate_keys}")
            return False
        # If no duplicates are found, log a success message
        logger.info(f"validate_primary_key_uniqueness validation passed: Primary key uniqueness verified for table '{table_name}' for entity_id '{entity_id}'.")
        return True
    except Exception as e:
        # Log unexpected errors and return False
        logger.error(f"Exception occurred during primary key validation for table '{table_name}', entity_id '{entity_id}': {str(e)}")
        return False



   

# Validate whether the foreign key constraints in the given DataFrame are properly enforced.
def validate_foreign_key_relationship(df, metadata, table_name, dfs, entity_id):
    """
    Validates foreign key constraints by ensuring that all foreign key values in the child table 
    exist in the referenced parent table.

    Args:
        df (DataFrame): The child table DataFrame to validate.
        metadata (dict): The metadata dictionary containing foreign key definitions.
        table_name (str): The name of the child table being validated.
        dfs (dict): A dictionary of DataFrames representing different tables in the dataset.
        entity_id (int): The entity identifier for logging purposes.

    Returns:
        bool: True if all foreign key constraints are satisfied, False otherwise.
    """
    try:
        # Retrieve foreign key constraints from metadata
        foreign_keys = fetch_metadata_property(metadata, table_name, "foreign_key")
        # If no foreign keys exist, log and return True (no validation needed)
        if not foreign_keys:
            logger.info(f"validate_foreign_key_relationship validation skipped: No foreign key relationships defined for table '{table_name}' for entity_id '{entity_id}'.")
            return True
        # Iterate through each foreign key relationship
        for child_column, reference in foreign_keys.items():
            parent_table = reference.get("table")
            parent_column = reference.get("column")
            # Check if the parent table exists in the provided DataFrames dictionary
            if parent_table not in dfs:
                logger.error(f"validate_foreign_key_relationship validation failed: Parent table '{parent_table}' not found in provided DataFrames for entity_id '{entity_id}'.")
                return False
            parent_df = dfs[parent_table]
            # Perform a left anti-join to find foreign key values in the child table that do not exist in the parent table
            missing_rows = df.join(parent_df, df[child_column] == parent_df[parent_column], "left_anti")
            missing_count = missing_rows.count()
            # If missing values are found, log an error and return False
            if missing_count > 0:
                missing_values = missing_rows.select(child_column).rdd.flatMap(lambda x: x).collect()
                logger.error(f"validate_foreign_key_relationship validation failed: {missing_count} missing foreign key values in '{table_name}.{child_column}' "
                             f"referencing '{parent_table}.{parent_column}' for entity_id '{entity_id}'. "
                             f"Missing values: {missing_values}")
                return False
        # If all foreign key relationships are valid, log a success message
        logger.info(f"validate_foreign_key_relationship validation passed: All foreign key relationships verified for table '{table_name}' "
                    f"for entity_id '{entity_id}'.")
        return True
    except Exception as e:
        # Log unexpected errors and return False
        logger.error(f"Exception occurred during foreign key validation for table '{table_name}', "
                     f"entity_id '{entity_id}': {str(e)}")
        return False



# Validation Configuration  required for apply_validation
VALIDATION_STEPS = [
    ("Column data type validation", validate_column_data_types),
    ("Nullable constraint validation", validate_nullable_constraint),
    ("Primary key uniqueness validation", validate_primary_key_uniqueness)
]



# Applies a series of validation checks to the given DataFrame based on predefined validation steps.
def apply_validation(filter_df, metadata, table_name, entity_id):
    try:
        # Initialize validation flag as True
        validation_passed = True        
        # Check if the DataFrame is empty, if not then only go for further validations
        if not check_empty_dataframe(filter_df, table_name, entity_id):
            logger.error("Validation process failed: DataFrame is empty. Skipping further validations.")
            return False        
        # Iterate through each validation function and apply it
        for validation_name, validation_func in VALIDATION_STEPS:
            try:
                result = validation_func(filter_df, metadata, table_name, entity_id)
                if not result:
                    logger.error(f"{validation_name} failed for table {table_name}.")
                    validation_passed = False
            except Exception as e:
                logger.error(f"Exception during {validation_name} for table {table_name}: {e}")
                validation_passed = False        
        # Log the overall validation result
        if validation_passed:
            logger.info(f"Validation process passed for table {table_name}.")
        else:
            logger.info(f"Validation process failed for table {table_name}.")        
        return validation_passed   
    # Return False in case of any unexpected exception   
    except Exception as e:
        logger.error(f"Unexpected error occurred during validation process for table {table_name}: {str(e)}")
        return False

# Generate dynamic validation list for each dtaframes to pass execute_validations function
def generate_validation(dfs,metadata,entity_id):
    validations = [(apply_validation, (df, metadata, table_name,entity_id)) for table_name, df in dfs.items()]
    validations.extend((validate_foreign_key_relationship, (df, metadata, table_name, dfs,entity_id)) for table_name, df in dfs.items())
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
                    logger.error("Metadata validation failed! Process further validations.")
                    validation_passed = False
            except Exception as e:
                logger.error(f"Exception during metadata validation: {str(e)}")
                validation_passed = False        
        # If any validation failed, log final error message and return False        
        if not validation_passed:
            logger.error("Some metadata validations failed. Please check the logs for details.\n Hence Metadata Validation process failed")
            return validation_passed        
        # Log success message if all validations passed
        logger.info("Metadata validation process completed successfully.")
        return validation_passed    
    # Return False in case of any unexpected exception
    except Exception as e:
        logger.error(f"Unexpected error occurred during metadata validation execution: {str(e)}")
        return False

 