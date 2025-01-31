
def null_check(df,column_name):
    """
    Check whether column in dataframe contains null valuess

    args:
    df: dataframe conatining data.
    column_name: column in dataframe to verify for null values.

    Steps:
        1. count the null values in a column in a dataframe.
        2. if count > 0:
            - collect all records for which column has null values.
            - return (null_record_df, false)
        3. else:
            - return (none,true)
    
    """

def empty_string_check(df,column_name):
    """
    check whether column in dataframe contains empty ("") values.

    args:
    df: dataframe conatining data.
    column_name: column in dataframe to verify for empty string values.

    Steps:
        1. count the empty string ("") values in a column in dataframe.
        2. if count > 0:
            - collect all records for which column has empty values.
            - return (empty_record_df, false)
        3. else:
            - return (none,true)
    """

def primary_key_uniqueness(df,primary_key_column):
    """
    check whether primary key column in dataframe contains unique values.

    args:
    df: dataframe conatining data.
    column_name: primary key column in dataframe.

    Steps:
        1. count the duplicate values in primary key column.
        2. also count null values in primary key column.
        3. if duplicate count or null count > 0:
            - collect all records for which column has null or duplicate values.
            - return (duplicate_record_df, false)
        3. else:
            - return (none,true)

    """

def duplicate_records_check(df):
    """
    check whether duplicate records exist in a dataframe.
    
    args:
    df : dataframe containing data.

    Steps:
        1. count the duplicate records in the dataframe.
        2. if count > 0:
            - collect all the duplicate records in df.
            - return (duplicate_record_df, false)
        3. else:
            - return (none,true)
    """

def duplicate_values_check(df,column_name):
    """
    ensures that there are no duplicate records in a column

    args:
    df: dataframe conatining data.
    column_name: primary key column in dataframe.

    Steps:
        1. count the duplicate values in column in df.
        2. if duplicate count > 0:
            - collect all records for which column has duplicate values.
            - return (duplicate_record_df, false)
        3. else:
            - return (none,true)
    """

def expected_value_check(df,column_name,expected_value):
    """
    ensures that values in a column matches to expected value.

    args:
    df: dataframe conatining data.
    column_name: column in dataframe.
    refrence_column: expected value to be checked for column.

    Steps:
        1. count the rows which don't contains expected value in a column.
        2. if count > 0:
            - collect all records that don't contain expected value.
            - return (invalid_record_df, false)
        3. else:
            - return (none,true)
    """

def date_format_check(df,column_name,date_format):
    """
    ensures that dates are matching the given format.

    args:
    df : dataframe containing data.
    column_name: column containing date.
    date_format: expected date format.

    Steps:
        1. count the all records for which date format is not matching.
        2. if count > 0:
            - collect the records for which date format is not matching.
            - return (invalid_record_df, false)
        3. else:
            - return (none,true)
    """


def min_value_check(df,column_name,min_value):
    """
    ensures that column contains value not less than min value.

    args:
    df: dataframe containing data.
    column_name: column to be verified for min value.
    min_value: minimum value expected.

    Steps:
        1. count records that has values less than min value.
        2. if count > 0:
            - collect all records that has value less than min value.
            - return (invalid_record_df, false)
        3. else:
            return (none, true)
    """
    
def max_value_check(df,column_name,max_value):
    """
    ensures that column contains value not more than max value
    args:
    df: dataframe containing data.
    column_name: column to be verified for max value.
    min_value: maximum value expected.

    Steps:
        1. count records that has values more than max value.
        2. if count > 0:
            - collect all records that has value more than max value.
            - return (invalid_record_df, false)
        3. else:
            return (none, true)
    """

def column_length_validation(df,column_name,length_):
    """
    ensures that values in column has specific length.

    args:
    df: dataframe containing data.
    column_name:  column to be verified for length.
    length_: expected length of value.

    Steps:
        1. count the values in column that don't have length equal to given length.
        2. if count > 0:
            - collect the invalid records.
            - return (invalid_record_df, false)
        3. else:
            return (none, true)
    """

