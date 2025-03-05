import sys
import os
import importlib
import time, hashlib
from functools import reduce
from pyspark.sql import Row, DataFrame
from pyspark.sql.functions import lit, col, collect_list
from Utilities.execution_result_saver import save_execution_result, save_invalid_records,save_valid_records
from common.constants import VAR_BAD_RECORD_PATH
from common.constants import EXECUTION_RESULTS_SCHEMA
from datetime import datetime
from common.custom_logger import *
#logger = getlogger()
import logging
logger = get_logger()

"""
        This function applies a list of data quality (DQ) rules to a given entity dataset.
        Steps:
        1. Iterates through the execution plan to extract rule details.
        2. Checks if the entity dataset is empty.
        3. Validates if the rule function exists in the 'Rules.inbuilt_rules' module.
        4. Calls the corresponding rule function dynamically with necessary parameters.
        5. Captures the results:
                - If the rule passes, logs execution status.
                - If it fails, saves error records and logs the failure.
                - If an exception occurs, logs the error and continues to the next rule.
        6. At the end of execution:
                - If all rules pass, saves good records.
                - If any rules fail, separates good and bad records, saving both.
                - If all rules cause exceptions, saves the records as they are.
        Returns:
        - `True` if all rules pass.
        - A track list indicating rule outcomes (pass/fail/exception).
        - `False` if an error occurs during execution.
"""

def execute_data_quality_rules(entity_data_df, execution_plans_list,spark):
        try:
                track_list = []
                failed_records_df_list = []
                entity_id = ""
                for plan in execution_plans_list:
                        var_rule_id = plan[1]
                        var_column_name = plan[3]
                        var_parameters = plan[4]
                        var_is_critical = plan[5]
                        var_rule_name = plan[7]
                        entity_id = plan[2]
                        var_plan_id = plan[0]
                        var_er_id = int(hashlib.sha256(f"{entity_id}-{var_plan_id}-{int(time.time() * 1000)}".encode()).hexdigest(), 16) % 900000 + 100000

                        if entity_data_df.count() == 0:
                                return f"Dataframe is Empty for entity_id {entity_id}.Make sure data exists for dataframe at source for entity_id {entity_id}.Status:'FAILED'"
                        
                        if not var_rule_name:
                                track_list.append(3)
                                logger.error(f"[DQ_RULE_EXECUTION] Rule {var_rule_name} with rule_id {var_rule_id} does not exists.Make sure rule name {var_rule_name} for rule_id {var_rule_id} exists in rule_master table")
                                continue

                        rules_module = importlib.import_module("Rules.inbuilt_rules")
                        if not hasattr(rules_module,var_rule_name):
                                track_list.append(3)
                                logger.error(f"[DQ_RULE_EXECUTION] Rule function {var_rule_name} for rule_id {var_rule_id} does not exists in {rules_module}, Skipping the rule. Please make sure function {var_rule_name} exists in module {rules_module}.")
                                continue

                        execution_result = {
                                "er_id":var_er_id,
                                "ep_id" : int(var_plan_id),
                                "rule_id" : int(var_rule_id),
                                "entity_id" : int(entity_id),
                                "column_name":var_column_name,
                                "is_critical" : var_is_critical,
                                "parameter_value" : var_parameters,
                                "total_records" : entity_data_df.count(),
                                "failed_records_count":0,
                                "er_status" : "SUCCESS",
                                "error_records_path" : "",
                                "error_message" : "",
                                "execution_timestamp" : str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                                "year" : str(datetime.now().year),
                                "month" : str(datetime.now().strftime("%m")),
                                "day" : str(datetime.now().strftime("%d"))
                        }
                        try:
                                rule_function = getattr(rules_module,var_rule_name)

                                if var_column_name and var_parameters:
                                        result = rule_function(entity_data_df, var_column_name, var_parameters)
                                elif var_column_name:
                                        result = rule_function(entity_data_df, var_column_name)
                                elif var_parameters:
                                        result = rule_function(entity_data_df, var_parameters)
                                else:
                                        result = rule_function(entity_data_df)

                                

                                if result[1]:
                                        track_list.append(2)
                                        row_data = Row(**execution_result)
                                        execution_result_df = spark.createDataFrame([row_data],EXECUTION_RESULTS_SCHEMA)
                                        logger.info(f"[DQ_RULE_EXECUTION] Saving the result, STATUS:{execution_result['er_status']}, EXECUTION_RESULT_ID:{var_er_id}")
                                        save_execution_result(execution_result_df,entity_id)

                                else:
                                        if result[0] == "EXCEPTION":
                                                track_list.append(3)
                                                logger.error(result[2])
                                                logger.debug(f"[DQ_RULE_EXECUTION] Skipping the application of rule {var_rule_name} for rule_id {var_rule_id} and plan id {var_plan_id}, Please check {rule_function} function logs.")
                                                continue
                                        
                                        failed_records_df = result[0]
                                        failed_rule = f"'{var_column_name}':'{var_rule_name}'"
                                        failed_records_df = failed_records_df.withColumn("failed_rules_info",lit(failed_rule))

                                        failed_records_df_list.append(failed_records_df)
                                        
                                        execution_result["er_status"] = "FAILED"
                                        execution_result["failed_records_count"] = failed_records_df.count()
                                        execution_result["error_message"] = result[2]
                                        execution_result["error_records_path"] = f"{VAR_BAD_RECORD_PATH}year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}/entity_id={entity_id}"
                                        
                                        row_data = Row(**execution_result)
                                        execution_result_df = spark.createDataFrame([row_data],EXECUTION_RESULTS_SCHEMA)
                                        logger.info(f"[DQ_RESULT_SAVE] Saving the result, STATUS:{execution_result['er_status']}, EXECUTION_RESULT_ID:{var_er_id}")
                                        save_execution_result(execution_result_df,entity_id)

                                        if var_is_critical == "Y":
                                                track_list.append(1)
                                                logger.error(f"[DQ_RULE_EXECUTION] critical rule {var_rule_name} failed for entity_id {entity_id}.")
                                        else:
                                                track_list.append(0)
                                                logger.error(f"[DQ_RULE_EXECUTION] non-critical rule {var_rule_name} failed for entity_id {entity_id}.")
                        except Exception as e:
                                track_list.append(3)
                                logger.error(f"[DQ_RULE_EXECUTION] Exception occured during application of Rule {var_rule_name}(rule_id={var_rule_id}): {e}")
                                continue
                if track_list:
                        passed_rules_count = track_list.count(2)
                        if len(set(track_list)) == 1 and track_list[0] == 2:
                                logger.info(f"[DQ_RULE_EXECUTION]  Rules execution has been completed successfully! Total Passed Rules: {passed_rules_count}, STATUS:'SUCCESS'")
                                save_valid_records(entity_data_df, entity_id)
                                return True
                        if failed_records_df_list:
                                failed_records_df = reduce(DataFrame.union, [df.select([col(c).cast("string") for c in df.columns]) for df in failed_records_df_list])
                                failed_records_df = failed_records_df.distinct()
                                groupby_col = failed_records_df.columns[0]
                                invalid_records_df = failed_records_df.groupBy(failed_records_df.columns[:-1]).agg(collect_list(col("failed_rules_info")).alias("failed_rules_info")).orderBy(groupby_col)
                                valid_records_df = entity_data_df.exceptAll(invalid_records_df.drop("failed_rules_info")).orderBy(groupby_col)
                                save_invalid_records(invalid_records_df,entity_id)
                                save_valid_records(valid_records_df,entity_id)
                                return track_list
                        else:
                                logger.info("[DQ_RULE_EXECUTION] Saving the records as-is, because all rules encountered exceptions for entity_id={entity_id}. STATUS:'FAILED'")
                                save_valid_records(entity_data_df, entity_id)
                                return track_list
                else:
                        logger.error("[DQ_RULE_EXECUTION] Rules are not processes correctly, please check output logs. STATUS:'FAILED'")
                        return False
                
        except Exception as e:
                logger.error(f"[DQ_RULE_EXECUTION] Exception occured in apply_rules(): {e}")
                return False




























"""
if track_list:
                        if len(set(track_list)) == 1 and track_list[0] == 2:
                                logger.info("DQ execution has been completed successfully!")
                                save_good_records(entity_data_df, entity_id)
                                return True
                        if all_df_list:
                                error_records_df = reduce(DataFrame.union, [df.select([col(c).cast("string") for c in df.columns]) for df in all_df_list])
                                error_records_df = error_records_df.distinct()
                                groupby_col = error_records_df.columns[0]
                                bad_records_df = error_records_df.groupBy(error_records_df.columns[:-1]).agg(collect_list(col("failed_rules")).alias("failed_rules")).orderBy(groupby_col)
                                #good_records_df = entity_data_df.filter(col(groupby_col).isNotNull()).join(bad_records_df, entity_data_df[groupby_col] == bad_records_df[groupby_col], "leftanti").orderBy(groupby_col)
                                good_records_df = entity_data_df.exceptAll(bad_records_df.drop("failed_rules")).orderBy(groupby_col)
                                save_bad_records(bad_records_df,entity_id)
                                save_good_records(good_records_df,entity_id)
                                return track_list
                        else:
                                logger.info("Saving the records as it is, beacuse exceptions occured for all rules for entity_id = {entity_id}")
                                save_good_records(entity_data_df, entity_id)
                                return track_list
                else:
                        logger.error("Rules are not processes correctly, please check output logs.")
                        return False
"""