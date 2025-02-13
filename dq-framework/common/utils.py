def fetch_execution_plan(execution_plan_with_rule_df):
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
        plan_list = [tuple(row) for row in execution_plan_with_rule_df.filter(execution_plan_with_rule_df.is_active == "Y").collect()]
        return plan_list
    except Exception as e:
        logger.error(f"Exception occured in fetch_execution_plan(): {e}")

def merge_plans_with_rules(execution_plan_df,rules_df):
    try:
        execution_plan_with_rule_df = execution_plan_df.join(rules_df.select("rule_id", "rule_name"), execution_plan_df.rule_id == rules_df.rule_id, "inner").drop(execution_plan_df.last_update_date).drop(rules_df.rule_id).orderBy("ep_id")
        return execution_plan_with_rule_df
    except Exception as e:
        logger.error(f"Exception occured in merge_plans_with_rules(): {e}")

# fetch path from entity master table path
def fetch_entity_path(entity_master_df,entity_id):
        """
        Extract the path where actual entity data is stored.
        args:entity_master_df,entity_id
        Steps:
            1. from entity master df fetch the file path where actual entity is stored.
            2. store the file path in a path variable
            path variable : a variable that contains the entity path fetched from df.
        """
    result = df.filter(col("entity_id") == entity_id).select("file_path").collect()
    
        if result:
            return result[0]["file_path"]
        return None

