import sys
import os
from common.constants import VAR_S3_EXECUTION_RESULT_PATH
from datetime import datetime
from common.constants import VAR_BAD_RECORD_PATH,VAR_GOOD_RECORD_PATH
from pyspark.sql.functions import lit
from common.custom_logger import *
#logger = getlogger()
import logging
logger = get_logger()

#Saves execution results of data quality checks to an Iceberg table in S3, partitioned by date and entity_id.
#Logs success or failure messages based on the save operation.
def save_execution_result(result_df,entity_id):
        try:
                # Write the result DataFrame to the Iceberg table in append mode with partitioning
                #result_df = result_df.withColumn("er_id", lit(None).cast("bigint"))
                result_df.write\
                        .format('iceberg')\
                        .mode('append')\
                        .saveAsTable(VAR_S3_EXECUTION_RESULT_PATH)
                # Log success message after successful save operation
                logger.info(f"[DQ_RESULT_SAVE] Result data saved for Entity id:{entity_id}. STATUS:'SUCCESS'")
        except Exception as e:
                # Log error if an exception occurs while saving the results
                logger.error(f"[DQ_RESULT_SAVE] Exception occured during saving the result records for entity_id={entity_id}: {e}")

#Saves records that fail data quality checks to a Parquet file in S3, partitioned by date and entity_id.
#Logs success or failure messages based on the save operation.
def save_invalid_records(error_records_df,entity_id):
        try:
                # Add entity_id and date-based partitioning columns to the DataFrame
                error_records_df = error_records_df.withColumn("entity_id",lit(entity_id))\
                                                .withColumn("year",lit(datetime.now().year))\
                                                .withColumn("month",lit(int(datetime.now().strftime("%m"))))\
                                                .withColumn("day",lit(int(datetime.now().strftime("%d"))))
                # Write the DataFrame to the storage location in append mode with partitioning
                error_records_df.write.mode('append')\
                        .partitionBy("year", "month", "day","entity_id")\
                        .format('parquet')\
                        .save(VAR_BAD_RECORD_PATH)
                # Log success message after saving bad records
                logger.info(f"[DQ_INVALID_RECORDS_SAVE] Invalid records saved for entity_id {entity_id}. STATUS:'SUCCESS'")
        except Exception as e:
                # Log error if an exception occurs while saving bad records
                logger.error(f"[DQ_INVALID_RECORDS_SAVE] Exception occured in save_bad_records() for entity_id={entity_id}: {e}")

#Saves records that pass data quality checks to a Parquet file in S3, partitioned by date and entity_id.
#Logs success or failure messages based on the save operation.
def save_valid_records(error_records_df,entity_id):
        try:
                # Add entity_id and date-based partitioning columns to the DataFrame
                error_records_df = error_records_df.withColumn("entity_id",lit(entity_id))\
                                        .withColumn("year",lit(datetime.now().year))\
                                        .withColumn("month",lit(int(datetime.now().strftime("%m"))))\
                                        .withColumn("day",lit(int(datetime.now().strftime("%d"))))
                # Write the DataFrame to the storage location in append mode with partitioning
                error_records_df.write.mode('append')\
                        .partitionBy("year", "month", "day","entity_id")\
                        .format('parquet')\
                        .save(VAR_GOOD_RECORD_PATH)
                # Log success message after saving good records
                logger.info(f"[DQ_VALID_RECORDS_SAVE] Valid records saved for entity_id {entity_id}. STATUS:'SUCCESS'")
        except Exception as e:
                # Log error if an exception occurs while saving good records
                logger.error(f"[DQ_VALID_RECORDS_SAVE] Exception occured in save_good_records() for entity_id={entity_id}: {e}")