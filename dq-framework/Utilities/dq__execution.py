import sys
import os
from common.utils import fetch_execution_plan
from Utilities.apply_rules import apply_rules


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
                logger.info("DQ execution has been completed successfully!")
                #print("DQ execution has been completed successfully!")
                return True
            logger.error("DQ execution has been failed!")
            #print("DQ execution has been failed!")
            return False
    
    except Exception as e:
        logger.error(f"Exception occured in dq_execution():{e}")
        return False

