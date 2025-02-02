import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from common.constants import VAR_ERROR_RECORD_PATH
from datetime import datetime
from pyspark.sql.functions import lit
import logging
logger = logging.getLogger()


def save_error_records(error_record_df,entity_id):
    """
    Save the error records to a location.

    args:
    error_record_df: dataframe containing records failed for a rule.
    entity_id: to store the datframe at entity level at path location.

    Steps:
        1. Create the path to store the df
        2. write the df at s3 error records path partioned as Y/M/D/entity_id.
    Output:
    path: a variable containing the path where error records are stored
    """
    try:
        path = VAR_ERROR_RECORD_PATH
        error_record_df = error_record_df.withColumn("entity_id",lit(entity_id))\
            .withColumn("year", lit(datetime.now().year))\
            .withColumn("month", lit(datetime.now().month)) \
            .withColumn("day", lit(datetime.now().day))
        error_record_df.write.mode('append').partitionBy("year", "month", "day", "entity_id").format('parquet').save(path)
        logger.info(f"Error records saved for Entity id={entity_id}")
    except Exception as e:
        logger.error(f"Exception occured during saving the error records for entity_id {entity_id}: {e}")


"""
entity_id = 10
path = f"{VAR_ERROR_RECORD_PATH}{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/{entity_id}"
print(path)
"""