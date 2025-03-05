import sys
from awsglue.utils import getResolvedOptions
from common import constants
from common.utils import *
from common.constants import *
from common.custom_logger import *
from common.spark_config import *
from Utilities.table_loader import *
from Utilities.validation import *
from Utilities.dq__execution import execute_data_quality_checks
# initialize logger
#logger = getlogger()

# take entity_id as input paramater and save it in constants
args = getResolvedOptions(sys.argv, ['entity_id','batch_id'])
constants.VAR_ENTITY_ID = args['entity_id']
VAR_BATCH_ID = args['batch_id']
logger = logger_init(VAR_BATCH_ID)

def main():

    #Initialize spark session
    spark=createSparkSession()
    
    # load config tables
    entity_master_df, execution_plan_df, execution_result_df, rule_master_df = fetch_tables(
        spark,VAR_S3_ENTITY_MASTER_PATH, VAR_S3_EXECUTION_PLAN_PATH, VAR_S3_EXECUTION_RESULT_PATH, VAR_S3_RULE_MASTER_PATH
    )

    #filter dataframes for entity_id
    entity_master_filtered_df = filter_config_by_entity(entity_master_df,VAR_ENTITY_ID)
    table_dataframes['dq_entity_master'] = entity_master_filtered_df
    execution_plan_filtered_df = filter_config_by_entity(execution_plan_df,VAR_ENTITY_ID)
    table_dataframes['dq_execution_plan'] = execution_plan_filtered_df

    #Filter rules from rule_master_df based on rule list fetch from execution_plan_df
    rule_list = fetch_rules(execution_plan_filtered_df)
    rule_master_filtered_df = fetch_filtered_rules(rule_list,rule_master_df)
    table_dataframes['dq_rule_master'] = rule_master_filtered_df

    # apply validation
    metadata = load_metadata(METADATA_PATH)
    validations = generate_validation(table_dataframes,metadata)
    validation_status = execute_validations(validations)
    if not validation_status:
        logger.error("Validation process has been failed, cannot proceed with apply rules process.")
        return False
    #fetch the entity file path from entity_master_df. fetch table from file_path
    entity_File_Path = fetch_entity_path(entity_master_df,VAR_ENTITY_ID)
    if not entity_File_Path:
        return False
    # load entity df
    entity_data_df=load_entity_data(entity_File_Path,spark)
    # apply dq
    execution_plans_with_rules_df = join_execution_plan_with_rules(execution_plan_filtered_df,rule_master_filtered_df)
    execute_data_quality_checks(execution_plans_with_rules_df,entity_data_df,spark)

if __name__ == "__main__":
    main()
