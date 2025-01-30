from utilities.execution_result_saver import save_results
from common.error_saver import save_error_records



def apply_rules(entity_data_df, rule_master_df,execution_plan_list):
        """
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


"""
import importlib

module = importlib.import_module("rules.inbuilt_rules")
rule_function = getattr(module,"null_check")

rule_function(df = [], column_name = "")
"""