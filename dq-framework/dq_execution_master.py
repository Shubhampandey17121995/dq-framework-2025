import sys
from awsglue.utils import getResolvedOptions
from common import constants
from common.custom_logger import getlogger
from common.utils import merge_plans_with_rules,fetch_rules,fetch_entity_path,fetch_filtered_rules
from common.constants import *
from common.custom_logger import getlogger
from common.spark_config import createSparkSession
from utilities.table_loader import config_loader,entity_data_loader,data_loader
from utilities.validation import *
from utilities.dq__execution import dq_execution
# initialize logger
logger = getlogger()
# take entity_id as input paramater and save it in constants
args = getResolvedOptions(sys.argv, ['entity_id'])
constants.VAR_ENTITY_ID = args['entity_id']

def main():

    #Initialize spark session
    spark=createSparkSession()
    
    # load config tables
    entity_master_df, execution_plan_df, execution_result_df, rule_master_df = entity_data_loader(
        spark,VAR_S3_ENTITY_MASTER_PATH, VAR_S3_EXECUTION_PLAN_PATH, VAR_S3_EXECUTION_RESULT_PATH, VAR_S3_RULE_MASTER_PATH
    )

    #filter dataframes for entity_id
    entity_master_filtered_df = config_loader(entity_master_df,VAR_ENTITY_ID)
    dfs['dq_entity_master'] = entity_master_filtered_df
    execution_plan_filtered_df = config_loader(execution_plan_df,VAR_ENTITY_ID)
    dfs['dq_execution_plan'] = execution_plan_filtered_df

    #Filter rules from rule_master_df based on rule list fetch from execution_plan_df
    rule_list = fetch_rules(execution_plan_filtered_df)
    rule_master_filtered_df=fetch_filtered_rules(rule_list,rule_master_df)
    dfs['df_rule_master'] = rule_master_filtered_df

    # apply validation
    metadata = load_required_metadata(DIRECTORY_PATH)
    validations = generate_validation(dfs,metadata)
    validation_status = execute_validations(validations)
    if not validation_status:
        logger.error("Validation process has been failed, cannot proceed with furthur process.")
        return False
    #fetch the entity file path from entity_master_df. fetch table from file_path
    entity_File_Path = fetch_entity_path(entity_master_df,VAR_ENTITY_ID)
    if not entity_File_Path:
        return False
    # load entity df
    entity_data_df=data_loader(entity_File_Path,spark)
    # apply dq
    execution_plan_with_rule_df = merge_plans_with_rules(execution_plan_filtered_df,rule_master_filtered_df)
    dq_execution(execution_plan_with_rule_df,entity_data_df,spark)


if __name__ == "__main__":
    main()
