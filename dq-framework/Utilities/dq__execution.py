import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from common.utils import fetch_execution_plan
from Utilities.apply_rules import apply_rules
import logging
logger = logging.getLogger()

"""
def dq_execution(execution_plan_df, entity_data_df, rule_master_df):
    
    In this function we will apply the dq on the actual data.

    args:
    execution_plan_df : dataframe that contains plan information for a entity.
    entity_data_df: dataframe on which we have to apply the dq.
    rule_master_df: to fetch names of the rules.

    Steps:
        1. Call the fetch_execution_plan() function.
            pass the execution_plan_df to function.
            This return the list of tuples of plans e.g. execution_plan_list[(rule_id, column_name, paramaters, is_critical, etc.)]
        2. Call the apply_rules() function.
            Pass the entity_data_df, rule_master_df,execution_plan_list to function.
            This function returns the track_list of rules passed/failed on the entity.
        
        3. count the passed and failed rules from track_list.(1= pass , 0= fail)
        
        4. print how many rules passed from total rules in list. Also log the result.
            
"""

def dq_execution(execution_plan_with_rule_df,entity_data_df,spark):
    try:
        plan_list = fetch_execution_plan(execution_plan_with_rule_df)
        
        result = apply_rules(entity_data_df,plan_list,spark)
        
        if isinstance(result,list):
            count_critical = result.count(1)
            count_non_critical = result.count(0)
            count_exceptions = result.count(3)
            count_success = result.count(2)
            logger.info(f"DQ Execution Summary: Critical Rules Failed: {count_critical}, Non-Critical Rules Failed: {count_non_critical}, Exceptions: {count_exceptions}, Success: {count_success}")
            logger.info(f"Hence Failing the process")
            return False
        
        elif isinstance(result,str):
            logger.error(result)
            logger.info("DQ EXECUTION FAILED!")
            return False
        
        elif isinstance(result, bool):
            if result:
                #logger.info("DQ execution has been completed successfully!")
                print("DQ execution has been completed successfully!")
                return True
            #logger.error("DQ execution has been failed!")
            print("DQ execution has been failed!")
            return False
    
    except Exception as e:
        logger.error(f"Exception occured in dq_execution():{e}")
        return False

