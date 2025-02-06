import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from functools import reduce
from pyspark.sql import Row, DataFrame
from pyspark.sql.functions import lit, col, collect_list, concat_ws
from Utilities.execution_result_saver import save_execution_result_records
from common.error_saver import save_bad_records, save_good_records
from common.constants import VAR_ERROR_RECORD_PATH
from common.constants import schema
from datetime import datetime
import importlib
import logging
logger = logging.getLogger()

"""
def apply_rules(entity_data_df, rule_master_df,execution_plan_list):

        This function will apply rules on actual_entity_data.
        The list contains the list of the plans.

        args:
        entity_data_df: dataframe on which rules to be applied
        rule_master_df: dataframe to fetch the rule name related to rule id
        execution_plan_list: list of tuples of plan info e.g. execution_plan_list[(rule_id, column_name, paramaters, is_critical, etc.)]

        Steps:
        1. define the track_list to keep track of pass and failed rules. this list will contain 0's and 1's.
        2. loop for each plan in execution_plan_list:
                1. Fetch execution plan data like rule_id,column_name,parameters into variables from each plan.
                2. Fetch the name of the rule from rule_master_df using above rule_id.Store it in a variable rule_name.
                3. Prepare the result_data dict for execution_results table.
                        e.g. result_data["er_status"] = "success" (same for other attributes in exec result table).
                4. Then according to rule_name execute the rule function on data.
                5. The rule function will return the tuple of result. If result is true it returns(none,true) else (error_record_df,false)
                6. Now we evaluate the result.
                        True:
                        
                        1. if the result is true then save_result().
                        2. we will pass the result_data_dict to this function.
                        3. this function save the result data at result location.
                        3. we will continue on next plan in the list

                        False:
                        
                        1. then set the result_data dict result related attributes as per fail result.
                        2. result_data["er_status"] = "FAIL" (same for other attributes in exec result table).
                        3. fetch the error_records_df from result e.g. error_record_df = result[0]
                        4. then call save_error_records() function and pass the above df.
                        5. this function store the error records at error record path.
                        6. above function will return the err records path, save it in result_data dict.
                        7. then call the save_result() function and pass the result_data dict.
                        8. Now we validate if result is critical or not:
                                If rule is critical, then log the result as critical rule failed.
                                        append 1 into the track list
                                If rule is not critical, then log the result as non-crtical rule failed.
                                        append 0 into the track list
                                Otherwise we continue the process on next plan.
                7. continue the process onto the next plan in the list
        4. return the track_list.
        
        Output:
        track_list: this list contains the 0's and 1's, where 0= rule failed and 1= rules passed.
"""



def apply_rules(entity_data_df, execution_plan_list,spark):
        try:
                track_list = []
                all_df_list = []
                entity_id = ""
                for plan in execution_plan_list:
                        var_rule_id = plan[1]
                        var_column_name = plan[3]
                        var_parameters = plan[4]
                        var_is_critical = plan[5]
                        var_rule_name = plan[7]
                        entity_id = plan[2]

                        if entity_data_df.count() == 0:
                                return f"Dataframe is Empty for entity_id {entity_id}.Make sure data exists for dataframe at source for entity_id {entity_id}"
                        
                        
                        #rule_name = rules_master_df.filter(rules_master_df.rule_id == var_rule_id).collect()[0][1]

                        module = importlib.import_module("Rules.inbuilt_rules")
                        if not hasattr(module,var_rule_name):
                                track_list.append(3)
                                logger.error(f"Rule function {var_rule_name} for rule_id {var_rule_id} does not exists in {module}, Skipping the rule. Please make sure function {var_rule_name} exists in module {module}.")
                                continue

                        result_data = {
                                "ep_id" : int(plan[0]),
                                "rule_id" : int(var_rule_id),
                                "entity_id" : int(entity_id),
                                "column_name":var_column_name,
                                "is_critical" : var_is_critical,
                                "parameter_value" : var_parameters,
                                "actual_value" : "",
                                "total_records" : entity_data_df.count(),
                                "failed_records_count":0,
                                "er_status" : "Pass",
                                "error_records_path" : "",
                                "error_message" : "",
                                "execution_timestamp" : datetime.now(),
                                "year" : datetime.now().year,
                                "month" : datetime.now().month,
                                "day" : datetime.now().day
                        }
                        try:
                                rule_function = getattr(module,var_rule_name)
                                
                                if var_column_name:
                                        logger.error(f"Column {var_column_name} not present in dataframe for entity_id {entity_id}.Make sure column name is correct or column exists in dataframe for entity id {entity_id}.")
                                        track_list.append(3)
                                        continue
                                        
                                if var_column_name and var_parameters:
                                        result = rule_function(entity_data_df, var_column_name, var_parameters, spark)
                                elif var_column_name:
                                        result = rule_function(entity_data_df, var_column_name, spark)
                                elif var_parameters:
                                        result = rule_function(entity_data_df, var_parameters, spark)
                                else:
                                        result = rule_function(entity_data_df, spark)

                                

                                if result[1]:
                                        track_list.append(2)
                                        row_data = Row(**result_data)
                                        result_df = spark.createDataFrame([row_data],schema)
                                        save_execution_result_records(result_df,entity_id)

                                else:
                                        if result[0] == "EXCEPTION":
                                                track_list.append(3)
                                                logger.error(result[2])
                                                logger.debug(f"Skipping the application of rule {var_rule_name} for rule_id {var_rule_id}, Please check {rule_function} function logs.")
                                                continue

                                        error_records_df = result[0]
                                        failed_rule = f"'{var_column_name}':'{var_rule_name}'"
                                        error_records_df = error_records_df.withColumn("failed_rules",lit(failed_rule))

                                        all_df_list.append(error_records_df)

                                        result_data["er_status"] = "Fail"
                                        result_data["failed_records_count"] = error_records_df.count()
                                        result_data["error_message"] = result[2]
                                        result_data["error_records_path"] = f"{VAR_ERROR_RECORD_PATH}year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}/entity_id={entity_id}"
                                        
                                        row_data = Row(**result_data)
                                        result_df = spark.createDataFrame([row_data],schema)
                                        save_execution_result_records(result_df,entity_id)
                                        
                                        #save_error_records(error_records_df, plan[2])
                                        
                                        if var_is_critical == "Y":
                                                track_list.append(1)
                                                logger.error(f"critical rule {var_rule_name} failed for {var_column_name} for entity_id {entity_id}.")
                                        else:
                                                track_list.append(0)
                                                logger.error(f"non-critical rule {var_rule_name} failed for {var_column_name} for entity_id {entity_id}.")
                        
                        except Exception as e:
                                track_list.append(3)
                                logger.error(f"Exception occured during application of Rule {var_rule_name} with rule_id={var_rule_id}: {e}")
                                continue
                if track_list:
                        if len(set(track_list)) == 1 and track_list[0] == 2:
                                logger.info("DQ execution has been completed successfully!")
                                save_good_records(entity_data_df, entity_id)
                                return True

                        if all_df_list:
                                error_records_df = reduce(DataFrame.union, all_df_list)
                                error_records_df.show(truncate = False)
                
                                groupby_col = error_records_df.columns[0]
                
                                merge_error_records_df = error_records_df.groupBy(error_records_df.columns[:-1]).agg(collect_list(col("failed_rules")).alias("failed_rules"))
                                merge_error_records_df.show(truncate=False)
                
                                good_records_df = entity_data_df.filter(col(groupby_col).isNotNull()).join(merge_error_records_df, entity_data_df[groupby_col] == merge_error_records_df[groupby_col], "leftanti").orderBy(groupby_col)
                                good_records_df.show(truncate=False)
                
                                save_bad_records(merge_error_records_df,entity_id)
                                save_good_records(good_records_df,entity_id)
                
                                return track_list
                        
                        else:
                                logger.info("Saving the records as it is, beacuse exceptions occured for all rules for entity_id = {entity_id}")
                                save_good_records(entity_data_df, entity_id)
                                return track_list
                else:
                        return False
                
        except Exception as e:
                logger.error(f"Exception occured in apply_rules(): {e}")
                return False
