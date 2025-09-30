import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import pandas as pd
from pyspark.sql.types import StringType, StructField, StructField
import openpyxl
import et_xmlfile
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1746565012580_df = pd.read_excel("s3://jevremd-datasets/hundred_megabyte.xlsx")
s3Source_node1_schema = StructType([StructField(column_name, StringType(), True) for column_name in AmazonS3_node1746565012580_df.columns])
AmazonS3_node1746565012580_df = spark.createDataFrame(AmazonS3_node1746565012580_df, schema=s3Source_node1_schema)
AmazonS3_node1746565012580 = DynamicFrame.fromDF(AmazonS3_node1746565012580_df, glueContext, "AmazonS3_node1746565012580")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=AmazonS3_node1746565012580, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746565001648", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746565014990 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1746565012580, connection_type="s3", format="glueparquet", connection_options={"path": "s3://jevremd-datasets", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1746565014990")

job.commit()