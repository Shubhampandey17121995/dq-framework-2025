import sys
from awsglue.utils import getResolvedOptions
from common import constants
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
args = getResolvedOptions(sys.argv, ['entity_id'])
constants.VAR_ENTITY_ID = args['entity_id']

def main():
    # load config tables

    # apply validation
    execute_validations(validations)
    # fetch entity path

    # load entity data
    
    # apply dq
    execution_plan_with_rule_df = merge_plans_with_rules(execution_plan_df,rule_master_df)
    dq_execution(execution_plan_with_rule_df,entity_data_df,spark)


if __name__ == "__main__":
    main()
