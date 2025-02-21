from pyspark.sql.functions import trim, col, length,to_date
from common.custom_logger import getlogger
logger = getlogger()

# Checks for null values in the specified column of the DataFrame.
# Returns a DataFrame of null records, a success flag, and an error message if nulls are found.
def null_check(df,column_name):
    try:
        # Filter rows where the specified column has null values and count the number of null records
        null_record_df = df.filter(df[column_name].isNull())
        null_count = null_record_df.count()
        
        if null_count > 0:
            # If null values are found, log an error message and return details
            error_message = f"Column '{column_name}' contains {null_count} null values."
            logger.error(error_message)
            # return df with records failed for check with flag and error message
            return (null_record_df, False, error_message)
        else:
            # If no null values are found, log success and return a success flag
            logger.info(f"Column '{column_name}' contains no null values.")
            return (None, True,None)
        
    except Exception as e:
        # Handle any unexpected exceptions and log the error
        logger.error(f"Exception occured during null_check rule: {e}")
        error_message = f"Exception:{e}"
        return ("EXCEPTION", False, error_message)

# Checks for empty string values in the specified column of the DataFrame.
# Returns a DataFrame of empty string records, a success flag, and an error message if empty strings are found.
def empty_string_check(df,column_name):
    try:
        # Count the number of records where the specified column contains empty strings after trimming spaces
        empty_count = df.filter(trim(df[column_name]) == "").count()
        if empty_count > 0:
            # If empty strings are found, filter and store those records
            empty_record_df = df.filter(trim(df[column_name]) == "")
            # Log the error and return details
            error_message = f"Column '{column_name}' contains {empty_count} empty string values."
            logger.error(error_message)
            # return df with records failed for check with flag and error messages
            return (empty_record_df, False, error_message)
        else:
            # If no empty strings are found, log success and return a success flag
            logger.info(f"Column '{column_name}' contains no empty string values.")
            return (None, True,None)
    except Exception as e:
        # Handle any unexpected exceptions and log the error
        logger.error(f"Exception occurred during empty string check: {e}")
        error_message = f"Exception:{e}"
        return ("EXCEPTION", False,error_message)

# Checks the uniqueness of a primary key column in the DataFrame.
# Identifies and returns duplicate and null primary key values if found, along with a success flag and an error message.
def primary_key_uniqueness(df,primary_key_column):
    try:
        # Identify duplicate primary key values (excluding NULLs)
        duplicate_keys_df = (
            df.filter(df[primary_key_column].isNotNull())   # Exclude NULLs for now
            .groupBy(primary_key_column)                    # Group by primary key column
            .count()
            .filter(col("count") > 1)                       # Filter values that appear more than once
            .select(primary_key_column)                     # Select only the primary key column
        )
        # Fetch duplicate records by joining with the DataFrame
        duplicate_record_df = df.join(duplicate_keys_df, on=primary_key_column, how="inner").dropDuplicates()
        duplicate_count = duplicate_record_df.count()
        # Identify NULL values in the primary key column
        null_record_df = df.filter(df[primary_key_column].isNull()).dropDuplicates()
        null_count = null_record_df.count()
        # Handle scenarios based on duplicates and NULL counts
        if duplicate_count > 0 and null_count > 0:
            error_message = f"Primary key column '{primary_key_column}' contains {duplicate_count} duplicate and {null_count} null values."
            logger.error(error_message)
            # return df with records failed for check with flag and error message
            return (duplicate_record_df.union(null_record_df), False, error_message)
        elif duplicate_count > 0:
            error_message = f"Primary key column '{primary_key_column}' contains {duplicate_count} duplicate values."
            logger.error(error_message)
            # return df with records failed for check with flag and error message
            return (duplicate_record_df, False, error_message)
        elif null_count > 0:
            error_message = f"Primary key column '{primary_key_column}' contains {null_count} null values."
            logger.error(error_message)
            # return df with records failed for check with flag and error message
            return (null_record_df, False, error_message)
        
        # If no duplicates or NULLs are found, return success
        logger.info(f"Primary key column '{primary_key_column}' contains no duplicate values.")
        return (None, True, None)
    except Exception as e:
        # Handle exceptions and log the error
        logger.error(f"Exception occurred during primary key uniqueness check: {e}")
        return ("EXCEPTION", False, f"Exception: {e}")

# Checks for duplicate records in the DataFrame.
# Returns a DataFrame of duplicate records, a success flag, and an error message if duplicates are found.
def duplicate_records_check(df):
    try:
        # Count the number of duplicate records by grouping all columns and filtering
        duplicate_count = df.groupBy(df.columns).count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            # Identify duplicate records
            duplicate_record_df = df.groupBy(df.columns).count().filter(col("count") > 1).drop("count")
                #duplicate_record_df = duplicate_record_df.join(df, on=df.columns, how='inner')
            # Log the error and return details
            error_message = f"Dataframe Contains {duplicate_count} duplicate records for ."
            logger.error(error_message)
            # return df with records failed for check with flag and error message
            return (duplicate_record_df, False, error_message)
        # If no duplicates are found, log success and return a success flag
        logger.info("Dataframe Contains no duplicate records.")
        return (None,True,None)
        
    except Exception as e:
        # Handle any unexpected exceptions and log the error
        logger.error(f"Exception occurred during duplicate records check: {e}")
        error_message = f"Exception:{e}"
        return ("EXCEPTION", False, error_message)

# Checks for duplicate values in a specific column of the DataFrame.
# Returns a DataFrame of duplicate values, a success flag, and an error message if duplicates are found.
def duplicate_values_check(df,column_name):
    try:
        # Count the number of duplicate values in the specified column
        duplicate_count = df.groupBy(column_name).count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            # Identify duplicate values by grouping on the column and filtering
            duplicate_record_df = df.groupBy(column_name).count().filter(col("count") > 1).drop("count")
            # Fetch records from the original DataFrame that have duplicate values
            duplicate_record_df = duplicate_record_df.join(df.distinct(), on=column_name, how='inner')
            # Log the error and return details
            error_message = f"Column '{column_name}' contains {duplicate_count} duplicate values."
            logger.error(error_message)
            # return df with records failed for check with flag and error message
            return (duplicate_record_df, False, error_message)
        # If no duplicates are found, log success and return a success flag
        logger.info(f"Column '{column_name}' contains no duplicate values.")
        return (None, True, None)
    
    except Exception as e:
        # Handle any unexpected exceptions and log the error
        logger.error(f"Exception occurred during duplicate values check: {e}")
        error_message = f"Exception:{e}"
        return ("EXCEPTION", False, error_message)

# Checks if all values in the specified column match the expected value.
# Returns a DataFrame of invalid records, a success flag, and an error message if mismatches are found.
def expected_value_check(df,column_name,expected_value):
    try:
        # Count records where the column value does not match the expected value
        invalid_count = df.filter(df[column_name] != expected_value).count()
        if invalid_count > 0:
            # Identify records with values different from the expected value or NULL values
            invalid_record_df = df.filter((df[column_name] != expected_value) | (df[column_name].isNull()))
            # Log the error and return details
            error_message = f"Column '{column_name}' contains {invalid_count} records with values different from the expected value '{expected_value}'."
            logger.error(error_message)
            # return df with records failed for check with flag and error message
            return (invalid_record_df, False, error_message)
        # If all values match the expected value, log success and return a success flag
        logger.info(f"Column '{column_name}' contains no records with values different from the expected value '{expected_value}'.")
        return (None, True, None)
    except Exception as e:
        # Handle any unexpected exceptions and log the error
        logger.error(f"Exception occurred during expected value check: {e}")
        error_message = f"Exception: {e}"
        return ("EXCEPTION", False, error_message)

# Validates whether the specified column follows the expected date format.
# Returns a DataFrame of invalid records, a success flag, and an error message if any invalid formats are found.
def date_format_check(df,column_name,date_format):
    try:
        # Attempt to parse the date column using the provided format
        parsed_date_df = df.withColumn("parsed_date", to_date(col(column_name), date_format))
        # Identify records where the date parsing failed
        invalid_date_df = parsed_date_df.filter(col("parsed_date").isNull())
        invalid_count = invalid_date_df.count()
        if invalid_count > 0:
            # Log and return details if invalid date formats exist
            error_message = f"Column '{column_name}' contains {invalid_count} records with invalid date format."
            logger.error(error_message)
            # return df with records failed for check with flag and error message
            return (invalid_date_df, False, error_message)
        else:
            # If all dates are valid, log success and return a success flag
            logger.info(f"Column '{column_name}' contains no records with invalid date format.")
            return (None, True,None)
    except Exception as e:
        # Handle unexpected exceptions and log the error
        logger.error(f"Exception occurred during date format check: {e}")
        error_message = f"Exception: {e}"
        return ("EXCEPTION", False, error_message)

# Checks if the values in the specified column meet the minimum value constraint.
# Returns a DataFrame of records violating the constraint, a success flag, and an error message if applicable.
def min_value_constraint_check(df,column_name,min_value):
    try:
        # Count records where the column value is less than the specified minimum
        min_value_count = df.filter(df[column_name] < min_value).count()
        
        if min_value_count > 0:
            # Filter records that violate the minimum value constraint
            min_value_df = df.filter(df[column_name] < min_value)
            # Log and return the error message along with the invalid records
            error_message = f"Column '{column_name}' contains {min_value_count} records with values less than the minimum value '{min_value}'."
            logger.error(error_message)
            # return df with records failed for check with flag and error message
            return (min_value_df, False, error_message)
        # If all values meet the constraint, log success and return a success flag
        logger.info(f"Column '{column_name}' contains no records with values less than the minimum value '{min_value}'.")
        return (None, True, None)
    
    except Exception as e:
        # Handle exceptions and log the error
        logger.error(f"Exception occurred during min value check: {e}")
        error_message = f"Exception: {e}"
        return ("EXCEPTION", False, error_message)

# Checks if the values in the specified column exceed the given maximum value.
# Returns a DataFrame of records violating the constraint, a success flag, and an error message if applicable.
def max_value_constraint_check(df,column_name,max_value):
    try:
        # Count records where the column value exceeds the maximum allowed value
        max_value_count = df.filter(df[column_name] > max_value).count()
        if max_value_count > 0:
            # Filter records that violate the maximum value constraint
            max_value_df = df.filter(df[column_name] > max_value)
            # Log and return the error message along with the invalid records
            error_message = f"Column '{column_name}' contains {max_value_count} records with values more than the maximum value '{max_value}'."
            logger.error(error_message)
            # return df with records failed for check with flag and error message
            return (max_value_df, False, error_message)
        # If all values meet the constraint, log success and return a success flag
        logger.info(f"Column '{column_name}' contains no records with values more than the maximum value '{max_value}'.")
        return (None, True, None)
    except Exception as e:
        # Handle exceptions and log the error
        logger.error(f"Exception occurred during min value check: {e}")
        error_message = f"Exception: {e}"
        return ("EXCEPTION", False, error_message)

# Checks if the values in the specified column have a length different from the expected length.
# Returns a DataFrame of records violating the constraint, a success flag, and an error message if applicable.
def column_length_check(df,column_name,length_):
    try:
        # Filter records where the column value length does not match the expected length
        invalid_records_df = df.filter(length(df[column_name]) != length_)
        invalid_count = invalid_records_df.count()
        if invalid_count > 0:
            # Log and return the error message along with the invalid records
            error_message = f"Column '{column_name}' contains {invalid_count} records with length not equal to {length_}."
            logger.error(error_message)
            # return df with records failed for check with flag and error message
            return (invalid_records_df, False, error_message)
        
        # If all values meet the constraint, log success and return a success flag
        logger.info(f"Column '{column_name}' contains no records with length not equal to {length_}.")
        return (None, True, None)
    except Exception as e:
        # Handle exceptions and log the error
        logger.error(f"Exception occurred during column length check: {e}")
        error_message = f"Exception: {e}"
        return ("EXCEPTION", False, error_message)

