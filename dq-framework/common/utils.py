# Databricks notebook source

# Function to load JSON from a file  with nullable info
def fetch_schema_with_nullable_from_file(file_path, table_name):
    """
    Extract column names and their nullable attribute from a JSON file for a given table.

    args:
    file_path (str): Path to the JSON file.
    table_name (str): The table name for which the schema needs to be fetched.
    
    Steps:
    1.Load json
    2.Fetch schema with nullable info
    3.Save result in "column_constraints"

    returns:
    dict: A dictionary where keys are column names and values are nullable attributes.
    """

# Function to load JSON from a file and extract schema with primary key columns
def fetch_primary_key_columns_from_file(file_path, table_name):
    """
    Extract column names with primary keys from a JSON file for a given table.

    args:
    file_path (str): Path to the JSON file.
    table_name (str): The table name for which the schema needs to be fetched.
    
    steps:    
    1.Load JSON file
    2.Extract schema for the given table  and Filter columns where primary_key is true
    3.Save result in "primary_keys" variable

    returns:
    list: A list of column names with primary key attributes.
    """

def fetch_execution_plan(execution_plan_df):
    """
        Extract the plans information for entity from execution plan df
        
        args:
        execution_plan_df : dataframe that contains plan information for a entity

        Steps:
            1. collect the rows of plans from dataframe
            2. create the list of tuples of execution plans available for entity.
                The rule_id, column_name, paramaters, is_critical, etc. will be fetched in the list.
            3. store the result in a list of tuples

        Output :
            list: A list of tuples of plan info e.g. execution_plan_list[(rule_id, column_name, paramaters, is_critical, etc.)]
    """
    try:
        plan_list = [tuple(row) for row in execution_plan_df.filter(execution_plan_df.is_active == "Y").collect()]
        return plan_list
    except Exception as e:
        print(f"Exception occured: {e}")

# fetch path from entity master table path
def fetch_entity_path(entity_master_df):
        """
        Extract the path where actual entity data is stored.

        args:
        entity_master_df : this dataframe contain the path

        Steps:
            1. from entity master df fetch the file path where actual entity is stored.
            2. store the file path in a path variable
        
        Output:
            path variable : a variable that contains the entity path fetched from df.
        """