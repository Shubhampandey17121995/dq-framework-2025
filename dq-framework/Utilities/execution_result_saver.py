import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from common.constants import VAR_EXECUTION_RESULT_PATH
from datetime import datetime
import logging
logger = logging.getLogger()


def save_result_records(result_df,entity_id):
        """
        Store the given result data at execution result table/location

        args:
        result_data_dict: a dictionary containing key:value pairs for the attributes in execution result table.

        Steps:
                1. create the df of result data dict.
                2. extract the entity id from result.
                3. create the path to store result.
                4. save the df at s3 location, partitioned by Y/M/D/entity_id
        """
        try:
                path = f"{VAR_EXECUTION_RESULT_PATH}{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/{entity_id}"
                result_df.write.mode('append').format('').save(path)
                logger.info(f"Result data saved for Entity id={entity_id}")
        except Exception as e:
                logger.error(f"Exception occured during saving the result records for entity_id {entity_id}.: {e}")


"""
entity_id = 10
path = f"{VAR_S3_EXECUTION_RESULT_PATH}{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/{entity_id}"
print(path)
"""