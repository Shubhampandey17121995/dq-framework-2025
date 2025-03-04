import sys
import os
from common.utils import get_active_execution_plans
from Utilities.apply_rules import execute_data_quality_rules
from common.custom_logger import getlogger
logger = getlogger()

"""
Executes data quality (DQ) validation by fetching the execution plan and applying rules
to the provided dataset. It categorizes the results into critical failures, non-critical
failures, exceptions, and successful rule executions. Logs a summary of the execution
and returns True if all rules pass, otherwise returns False.
"""
def execute_data_quality_checks(execution_plan_with_rules_df,entity_data_df,spark):
    try:
        # Fetch the execution plan as a list of rules to be applied
        execution_plans_list = get_active_execution_plans(execution_plan_with_rules_df)
        # Apply rules on the entity data
        dq_execution_result = execute_data_quality_rules(entity_data_df,execution_plans_list,spark)
        # If result is a list, count occurrences of different rule validation statuses
        if isinstance(dq_execution_result,list):
            critical_failures = dq_execution_result.count(1)        # Critical rule failures
            non_critical_failures = dq_execution_result.count(0)    # Non-critical rule failures
            execution_exceptions = dq_execution_result.count(3)      # Exceptions encountered
            successful_checks = dq_execution_result.count(2)         # Successful validations
            # Log the execution summary
            logger.info(f"[DQ_CHECK_COMPLETED] DQ Execution Summary: Critical Rules Failed: {critical_failures}, Non-Critical Rules Failed: {non_critical_failures}, Exceptions: {execution_exceptions}, Success: {successful_checks}, Status:'FAILED'")
            # Log the process failure due to rule violations
            logger.info(f"[DQ_CHECK_COMPLETED]Hence Failing the process")
            return False
        # If result is a string, log the error message and fail execution
        elif isinstance(dq_execution_result,str):
            logger.error(dq_execution_result)
            logger.info("[DQ_CHECK_COMPLETED] DQ EXECUTION FAILED!")
            return False
        # If result is a boolean, determine success or failure
        elif isinstance(dq_execution_result, bool):
            if dq_execution_result:
                logger.info("[DQ_CHECK_COMPLETED] DQ execution has been completed successfully!")
                return True
            logger.error("[DQ_CHECK_COMPLETED] DQ execution has been failed!")
            return False
    except Exception as e:
        # Handle any unexpected exceptions and log the error
        logger.error(f"Exception occurred in execute_data_quality_checks():{e}")
        return False

