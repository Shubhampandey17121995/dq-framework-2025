from pyspark.sql import SparkSession
from common.custom_logger import getlogger
from common.constants import *
logger = getlogger()

"""
def createSparkSession():
    spark = SparkSession.builder \
        .appName("IcebergTableReader") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.job_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.job_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.job_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.job_catalog.warehouse", "s3://dq-framework-tables/DQ_TABLES/") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
        .getOrCreate()
    logger.info("Created spark session")
    return spark
"""

def createSparkSession():
    spark = SparkSession.builder \
        .appName("IcebergTableReader") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
        .config("spark.sql.catalog.s3tablesbucket.warehouse", MY_TABLE_BUCKET_ARN) \
        .getOrCreate()
    logger.info("Created spark session")
    return spark