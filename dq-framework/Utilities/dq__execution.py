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
            if not result:
                logger.info("DQ execution has been completed successfully!")
                return True
            else:
                if result.count(1) > 0:
                    logger.error(f"DQ execution has been failed for critical rules. {result.count(1)} critical rules failed out of {len(plan_list)} rules. Hence failing the process")
                    return False
                elif result.count(0) > 0:
                    logger.error(f"DQ execution has been failed for non-critical rules. {result.count(0)} non-critical rules failed out of {len(plan_list)} rules. Hence failing the process")
                    return False
        
        elif isinstance(result,str):
            logger.error(result)
            return False
        
        logger.error("DQ EXECUTION FAILED!")
        return result
    
    except Exception as e:
        logger.error(f"Exception occured in dq_execution():{e}")
        return False

