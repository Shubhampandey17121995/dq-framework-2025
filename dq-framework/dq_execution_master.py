import sys
from awsglue.utils import getResolvedOptions
from common.custom_logger import getlogger
from common.utils import *
from common.constants import *
from common.validation_config import *
from common.custom_logger import *
from common.spark_config import *
from Utilities.table_loader import *
from Utilities.validation import *
from Utilities.dq__execution import *
logger = getlogger()

def main():
    # get entity id
    args = getResolvedOptions(sys.argv, ['entity_id'])
    entity_id = args['entity_id']
    # Step 1: Get custom logger.

    #Step 2: Initialize the spark session.
    createSparkSession()

    #Step 3: Load configuration from configuration table in a df => rule_master_df, entity_master_df, execution_plan_df.
    entity_master_df, execution_plan_df, execution_result_df, rule_master_df = entity_data_loader(
        VAR_S3_ENTITY_MASTER_PATH, VAR_S3_EXECUTION_PLAN_PATH, VAR_S3_EXECUTION_RESULT_PATH, VAR_S3_RULE_MASTER_PATH
    )

    # apply validation
    execute_validations(validations)
    
    #Step 4: Filter entity and load entity data in df => entity_data_df.
    #filter dataframes for entity_id
    entity_master_filtered_df = config_loader(VAR_S3_ENTITY_MASTER_PATH,ENTITY_ID)
    execution_plan_filtered_df = config_loader(VAR_S3_EXECUTION_PLAN_PATH,ENTITY_ID)
    rule_master_filtered_df = config_loader(VAR_S3_RULE_MASTER_PATH,ENTITY_ID)


    #Step 5: fetch the entity file path from entity_master_df.
    Entity_File_Path = fetch_entity_path(entity_master_df,entity_id)
    
    # apply dq
    execution_plan_with_rule_df = merge_plans_with_rules(execution_plan_df,rule_master_df)
    dq_execution(execution_plan_with_rule_df,entity_data_df,spark)


if __name__ == "__main__":
    main()


"""
    Step 7: Execute the DQ framework.

        In this step we call the dq_execution() function.
        It takes following parameters:
            execution_plan_df : dataframe that contains plan information for a entity.
            entity_data_df: dataframe on which we have to apply the rules.
            rule_master_df: to fetch names of the rules.
        
        1. Call the fetch_execution_plan() function
            This function extracts the plans information for entity from execution plan df
            1.1 this function takes following parameters:
                execution_plan_df : dataframe that contains plan information for a entity
            1.2 function extracts the plans information for entity from execution plan df
            1.2 return the list of tuples of plans e.g. execution_plan_list[(rule_id, column_name, paramaters, is_critical, etc.)]

        2. Call the apply_rules() function.
            This function will apply rules on actual_entity_data.
            2.1 this function takes following parameters:
			    entity_data_df: dataframe on which rules to be applied
			    rule_master_df: dataframe to fetch the rule name related to rule id
			    execution_plan_list: list of tuples of plan info e.g. execution_plan_list[(rule_id, column_name, paramaters, is_critical, etc.)]
            2.2 for each plan in list it applies the rules on the column in entity_data_df
            2.3 it keeps the track of passed and failed rules in a list.
            2.5 this function returns the list of rules passed/failed on the entity.

        3. count the passed and failed rules from track_list.(1= pass , 0= fail)

        4. print how many rules passed from total rules in track_list.
"""