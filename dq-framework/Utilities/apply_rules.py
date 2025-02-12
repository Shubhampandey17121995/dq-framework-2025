import sys
import os
import importlib
from functools import reduce
from pyspark.sql import Row, DataFrame
from pyspark.sql.functions import lit, col, collect_list
from Utilities.execution_result_saver import save_execution_result_records, save_bad_records,save_good_records
from common.constants import VAR_BAD_RECORD_PATH
from common.constants import schema
from datetime import datetime

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
                        
                        if not var_rule_name:
                                track_list.append(3)
                                logger.error(f"Rule {var_rule_name} with rule_id={var_rule_id} does not exists.Make sure rule {var_rule_name} exists in rule_master table")
                                continue

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
                                        result_data["error_records_path"] = f"{VAR_BAD_RECORD_PATH}year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}/entity_id={entity_id}"
                                        
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
                        return False
                
        except Exception as e:
                logger.error(f"Exception occured in apply_rules(): {e}")
                return False
