import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from common.constants import VAR_BAD_RECORD_PATH,VAR_GOOD_RECORD_PATH
from datetime import datetime
from pyspark.sql.functions import lit
import logging
logger = logging.getLogger()


# save records that don't pass the rule
def save_bad_records(error_records_df,entity_id):
    try:
        path = VAR_BAD_RECORD_PATH
        error_records_df = error_records_df.withColumn("entity_id",lit(entity_id))\
                                .withColumn("year",lit(datetime.now().year))\
                                .withColumn("month",lit(datetime.now().month))\
                                .withColumn("day",lit(datetime.now().day))
        
        error_records_df.write.mode('append').partitionBy("year", "month", "day","entity_id").format('parquet').save(path)
        logger.info(f"Bad rror records saved for entity_id {entity_id}")
    except Exception as e:
        logger.error(f"Exception occured in save_bad_records() for entity_id={entity_id}: {e}")

# save records that passed the rule
def save_good_records(error_records_df,entity_id):
    try:
        path = VAR_GOOD_RECORD_PATH
        error_records_df = error_records_df.withColumn("entity_id",lit(entity_id))\
                                .withColumn("year",lit(datetime.now().year))\
                                .withColumn("month",lit(datetime.now().month))\
                                .withColumn("day",lit(datetime.now().day))
        
        error_records_df.write.mode('append').partitionBy("year", "month", "day","entity_id").format('parquet').save(path)
        logger.info(f"Good records saved for entity_id {entity_id}")
    except Exception as e:
        logger.error(f"Exception occured in save_good_records() for entity_id={entity_id}: {e}")

