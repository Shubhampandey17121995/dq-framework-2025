from pyspark.sql.functions import trim, col, length

def null_check(df,column_name):
    try:
        null_record_df = df.filter(df[column_name].isNull())
        null_count = null_record_df.count()
        
        if null_count > 0:
            error_message = f"Column '{column_name}' contains {null_count} null values."
            logger.error(error_message)
            return (null_record_df, False, error_message)
        else:
            logger.info(f"Column '{column_name}' contains no null values.")
            return (None, True,None)
        
    except Exception as e:
        logger.error(f"Exception occured during null check: {e}")
        error_message = f"Exception:{e}"
        return ("EXCEPTION", False, error_message)
#*****************************************************
def empty_string_check(df,column_name):
    try:
        empty_count = df.filter(trim(df[column_name]) == "").count()
        
        if empty_count > 0:
            empty_record_df = df.filter(trim(df[column_name]) == "")
            error_message = f"Column '{column_name}' contains {empty_count} empty string values."
            logger.error(error_message)
            return (empty_record_df, False, error_message)
        else:
            logger.info(f"Column '{column_name}' contains no empty string values.")
            return (None, True,None)
        
    except Exception as e:
        logger.error(f"Exception occurred during empty string check: {e}")
        error_message = f"Exception:{e}"
        return ("EXCEPTION", False,error_message)
#*****************************************************
def primary_key_uniqueness(df,primary_key_column):
    try:
        duplicate_count = df.groupBy(primary_key_column).count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            duplicate_record_df = df.groupBy(primary_key_column).count().filter(col("count") > 1).drop("count")
            duplicate_record_df = duplicate_record_df.join(df, on=primary_key_column, how='inner')
            error_message = f"Primary key column '{primary_key_column}' contains {duplicate_count} duplicate values."
            logger.error(error_message)
            return (duplicate_record_df, False, error_message)
        
        null_count = df.filter(df[primary_key_column].isNull()).count()
        if null_count > 0:
            null_record_df = df.filter(df[primary_key_column].isNull())
            error_message = f"Primary key column '{primary_key_column}' contains {null_count} null values."
            logger.error(error_message)
            return (null_record_df, False, error_message)
        
        logger.info(f"Primary key column '{primary_key_column}' contains no duplicate values.")
        return (None, True,None)
    
    except Exception as e:
        logger.error(f"Exception occurred during primary key uniqueness check: {e}")
        error_message = f"Exception:{e}"
        return ("EXCEPTION", False, error_message)
#*****************************************************
def duplicate_records_check(df):
    try:
        duplicate_count = df.groupBy(df.columns).count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            duplicate_record_df = df.groupBy(df.columns).count().filter(col("count") > 1).drop("count")
            duplicate_record_df = duplicate_record_df.join(df, on=df.columns, how='inner')
            error_message = f"Dataframe Contains {duplicate_count} duplicate records for ."
            logger.error(error_message)
            return (duplicate_record_df, False, error_message)
        
        logger.info("Dataframe Contains no duplicate records.")
        return (None,True,None)
        
    except Exception as e:
        logger.error(f"Exception occurred during duplicate records check: {e}")
        error_message = f"Exception:{e}"
        return ("EXCEPTION", False, error_message)
#*****************************************************
def duplicate_values_check(df,column_name):
    try:
        duplicate_count = df.groupBy(column_name).count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            duplicate_record_df = df.groupBy(column_name).count().filter(col("count") > 1).drop("count")
            duplicate_record_df = duplicate_record_df.join(df, on=column_name, how='inner')
            error_message = f"Column '{column_name}' contains {duplicate_count} duplicate values."
            logger.error(error_message)
            return (duplicate_record_df, False, error_message)
        
        logger.info(f"Column '{column_name}' contains no duplicate values.")
        return (None, True, None)
    
    except Exception as e:
        logger.error(f"Exception occurred during duplicate values check: {e}")
        error_message = f"Exception:{e}"
        return ("EXCEPTION", False, error_message)
#*****************************************************
def expected_value_check(df,column_name,expected_value):
    try:
        invalid_count = df.filter(df[column_name] != expected_value).count()

        if invalid_count > 0:
            invalid_record_df = df.filter(df[column_name] != expected_value)
            error_message = f"Column '{column_name}' contains {invalid_count} records with values different from the expected value '{expected_value}'."
            logger.error(error_message)
            return (invalid_record_df, False, error_message)
        
        logger.info(f"Column '{column_name}' contains no records with values different from the expected value '{expected_value}'.")
        return (None, True, None)
    
    except Exception as e:
        logger.error(f"Exception occurred during expected value check: {e}")
        error_message = f"Exception: {e}"
        return ("EXCEPTION", False, error_message)
#*****************************************************
def date_format_check(df,column_name,date_format):
    try:
        parsed_date_df = df.withColumn("parsed_date", to_date(col(column_name), date_format))
        invalid_date_df = parsed_date_df.filter(col("parsed_date").isNull())
        invalid_count = invalid_date_df.count()
        if invalid_count > 0:
            error_message = f"Column '{column_name}' contains {invalid_count} records with invalid date format."
            logger.error(error_message)
            return (invalid_date_df, False, error_message)
        else:
            logger.info(f"Column '{column_name}' contains no records with invalid date format.")
            return (None, True,None)
    
    except Exception as e:
        logger.error(f"Exception occurred during date format check: {e}")
        error_message = f"Exception: {e}"
        return ("EXCEPTION", False, error_message)
#*****************************************************
def min_value_constraint_check(df,column_name,min_value):
    try:
        min_value_count = df.filter(df[column_name] < min_value).count()
        
        if min_value_count > 0:
            min_value_df = df.filter(df[column_name] < min_value)
            error_message = f"Column '{column_name}' contains {min_value_count} records with values less than the minimum value '{min_value}'."
            logger.error(error_message)
            return (min_value_df, False, error_message)
        
        logger.info(f"Column '{column_name}' contains no records with values less than the minimum value '{min_value}'.")
        return (None, True, None)
    
    except Exception as e:
        logger.error(f"Exception occurred during min value check: {e}")
        error_message = f"Exception: {e}"
        return ("EXCEPTION", False, error_message)
#*****************************************************
def max_value_check(df,column_name,max_value):
    try:
        max_value_count = df.filter(df[column_name] > max_value).count()
        
        if max_value_count > 0:
            max_value_df = df.filter(df[column_name] > max_value)
            error_message = f"Column '{column_name}' contains {max_value_count} records with values more than the maximum value '{max_value}'."
            logger.error(error_message)
            return (max_value_df, False, error_message)
        
        logger.info(f"Column '{column_name}' contains no records with values more than the maximum value '{max_value}'.")
        return (None, True, None)
    
    except Exception as e:
        logger.error(f"Exception occurred during min value check: {e}")
        error_message = f"Exception: {e}"
        return ("EXCEPTION", False, error_message)
#*****************************************************
def column_length_check(df,column_name,length_):
    try:
        invalid_records_df = df.filter(length(df[column_name]) != length_)
        invalid_count = invalid_records_df.count()
        
        if invalid_count > 0:
            error_message = f"Column '{column_name}' contains {invalid_count} records with length not equal to {length_}."
            logger.error(error_message)
            return (invalid_records_df, False, error_message)
        
        logger.error(f"Column '{column_name}' contains no records with length not equal to {length_}.")
        return (None, True, None)
    
    except Exception as e:
        logger.error(f"Exception occurred during column length check: {e}")
        error_message = f"Exception: {e}"
        return ("EXCEPTION", False, error_message)

