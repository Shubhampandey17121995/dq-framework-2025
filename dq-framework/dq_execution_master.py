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
#args = getResolvedOptions(sys.argv, ['entity_id'])
#entity_id = args['entity_id']


def main():
    # Step 1: Get custom logger.

    #Step 2: Initialize the spark session.
    spark=createSparkSession()

    #Step 3: Load configuration from configuration table in a df => rule_master_df, entity_master_df, execution_plan_df.
    entity_master_df, execution_plan_df, execution_result_df, rule_master_df = entity_data_loader(
        spark,VAR_S3_ENTITY_MASTER_PATH, VAR_S3_EXECUTION_PLAN_PATH, VAR_S3_EXECUTION_RESULT_PATH, VAR_S3_RULE_MASTER_PATH
    )
    logger.info("DF loaded Successfully")

    #Step 4: Filter entity and load entity data in df => entity_data_df.
    #filter dataframes for entity_id
    entity_master_filtered_df = config_loader(entity_master_df,ENTITY_ID)
    execution_plan_filtered_df = config_loader(execution_plan_df,ENTITY_ID)

    #description:Filter rules from rule_master_df based on rule list fetch from execution_plan_df
    rule_list = fetch_rules(execution_plan_filtered_df)

    rule_master_filtered_df=fetch_filtered_rules(rule_list,rule_master_df)

    

    # apply validation
    execute_validations(validations)


    #Step 5: fetch the entity file path from entity_master_df. fetch table from file_path
    Entity_File_Path = fetch_entity_path(entity_master_df,entity_id)
    entity_data_df=data_loader(Entity_File_Path)
    

    # apply dq
    execution_plan_with_rule_df = merge_plans_with_rules(execution_plan_df,rule_master_filtered_df)

    dq_execution(execution_plan_with_rule_df,entity_data_df,spark)


if __name__ == "__main__":
    main()
