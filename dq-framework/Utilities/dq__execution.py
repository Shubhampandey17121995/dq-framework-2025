import sys
import os
from common.utils import fetch_execution_plan
from utilities.apply_rules import apply_rules
from common.custom_logger import getlogger
logger = getlogger()

"""
Executes data quality (DQ) validation by fetching the execution plan and applying rules
to the provided dataset. It categorizes the results into critical failures, non-critical
failures, exceptions, and successful rule executions. Logs a summary of the execution
and returns True if all rules pass, otherwise returns False.
"""
def dq_execution(execution_plan_with_rule_df,entity_data_df,spark):
    try:
        # Fetch the execution plan as a list of rules to be applied
        plan_list = fetch_execution_plan(execution_plan_with_rule_df)
        # Apply rules on the entity data
        result = apply_rules(entity_data_df,plan_list,spark)
        # If result is a list, count occurrences of different rule validation statuses
        if isinstance(result,list):
            count_critical = result.count(1)        # Critical rule failures
            count_non_critical = result.count(0)    # Non-critical rule failures
            count_exceptions = result.count(3)      # Exceptions encountered
            count_success = result.count(2)         # Successful validations
            # Log the execution summary
            logger.info(f"DQ Execution Summary: Critical Rules Failed: {count_critical}, Non-Critical Rules Failed: {count_non_critical}, Exceptions: {count_exceptions}, Success: {count_success}")
            # Log the process failure due to rule violations
            logger.info(f"Hence Failing the process")
            return False
        # If result is a string, log the error message and fail execution
        elif isinstance(result,str):
            logger.error(result)
            logger.info("DQ EXECUTION FAILED!")
            return False
        # If result is a boolean, determine success or failure
        elif isinstance(result, bool):
            if result:
                logger.info("DQ execution has been completed successfully!")
                return True
            logger.error("DQ execution has been failed!")
            return False
    except Exception as e:
        # Handle any unexpected exceptions and log the error
        logger.error(f"Exception occured in dq_execution():{e}")
        return False

