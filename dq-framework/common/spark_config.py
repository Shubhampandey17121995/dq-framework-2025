#Initialize SparkSession
def createSparkSession():
    """
    step 1: set spark configuration for aws s3
    step 2: return sparksession
    """
    spark = SparkSession.builder \
        .appName("IcebergTableReader") \
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME, SPARK_CATALOG_IMPL) \
        .config("spark.sql.catalog." + SPARK_CATALOG_NAME + ".warehouse", SPARK_CATALOG_WAREHOUSE) \
        .config("spark.sql.extensions", SPARK_EXTENSIONS) \
        .config("spark.jars.packages", SPARK_JARS_PACKAGES) \
        .getOrCreate()
    return spark



























"""
import sys
import os
import time
from pyspark.sql import SparkSession
#Replace <aws_region> with AWS region name.
#Replace <aws_account_id> with AWS account ID.
def createSparkSession():
    spark = SparkSession.builder.appName('osspark') \
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160') \
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config('spark.sql.defaultCatalog', 'spark_catalog') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkCatalog') \
    .config('spark.sql.catalog.spark_catalog.type', 'rest') \
    .config('spark.sql.catalog.spark_catalog.uri','https://glue.<aws_region>.amazonaws.com/iceberg') \
    .config('spark.sql.catalog.spark_catalog.warehouse','<aws_account_id>') \
    .config('spark.sql.catalog.spark_catalog.rest.sigv4-enabled','true') \
    .config('spark.sql.catalog.spark_catalog.rest.signing-name','glue') \
    .config('spark.sql.catalog.spark_catalog.rest.signing-region', <aws_region>) \
    .config('spark.sql.catalog.spark_catalog.io-impl','org.apache.iceberg.aws.s3.S3FileIO') \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialProvider') \
    .config('spark.sql.catalog.spark_catalog.rest-metrics-reporting-enabled','false') \
    .getOrCreate()
    return spark
"""
