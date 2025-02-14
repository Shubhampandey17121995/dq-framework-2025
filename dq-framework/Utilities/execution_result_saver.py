import sys
import os
from common.constants import VAR_S3_EXECUTION_RESULT_PATH
from datetime import datetime
from common.constants import VAR_BAD_RECORD_PATH,VAR_GOOD_RECORD_PATH
from pyspark.sql.functions import lit

#Saves execution results of data quality checks to an Iceberg table in S3, partitioned by date and entity_id.  
#Logs success or failure messages based on the save operation.
def save_execution_result_records(result_df,entity_id):
        try:
                path = VAR_S3_EXECUTION_RESULT_PATH
                result_df.write.mode('append').partitionBy("year", "month", "day", "entity_id").format('iceberg').save(path)
                logger.info(f"Result data saved for Entity id:{entity_id}")
        except Exception as e:
                logger.error(f"Exception occured during saving the result records for entity_id={entity_id}: {e}")


#Saves records that fail data quality checks to a Parquet file in S3, partitioned by date and entity_id.  
#Logs success or failure messages based on the save operation.
def save_bad_records(error_records_df,entity_id):
        try:
                path = VAR_BAD_RECORD_PATH
                error_records_df = error_records_df.withColumn("entity_id",lit(entity_id))\
                                        .withColumn("year",lit(datetime.now().year))\
                                        .withColumn("month",lit(int(datetime.now().strftime("%m"))))\
                                        .withColumn("day",lit(int(datetime.now().strftime("%d"))))

                error_records_df.write.mode('append').partitionBy("year", "month", "day","entity_id").format('parquet').save(path)
                logger.info(f"Bad rror records saved for entity_id {entity_id}")
        except Exception as e:
                logger.error(f"Exception occured in save_bad_records() for entity_id={entity_id}: {e}")


#Saves records that pass data quality checks to a Parquet file in S3, partitioned by date and entity_id.  
#Logs success or failure messages based on the save operation.
def save_good_records(error_records_df,entity_id):
        try:
                path = VAR_GOOD_RECORD_PATH
                error_records_df = error_records_df.withColumn("entity_id",lit(entity_id))\
                                        .withColumn("year",lit(datetime.now().year))\
                                        .withColumn("month",lit(int(datetime.now().strftime("%m"))))\
                                        .withColumn("day",lit(int(datetime.now().strftime("%d"))))

                error_records_df.write.mode('append').partitionBy("year", "month", "day","entity_id").format('parquet').save(path)
                logger.info(f"Good records saved for entity_id {entity_id}")
        except Exception as e:
                logger.error(f"Exception occured in save_good_records() for entity_id={entity_id}: {e}")