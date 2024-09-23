import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME"])  # Use sys.argv

# Initialize Glue context and Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Define source and target paths
source_path = "s3://demo-source-ap-south-1-1809/"
target_path = "s3://demo-transform-ap-south-1-1809/data/"

# Read data from CSV into Glue DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [source_path], "recurse": True},
    transformation_ctx="dynamic_frame",
)

# Convert DynamicFrame to DataFrame
data_frame = dynamic_frame.toDF()

# Perform transformations if needed
# Example: Filter rows where 'some_column' is greater than some_value
# data_frame = data_frame.filter(data_frame['some_column'] > some_value)

# Write DataFrame to Parquet format

data_frame.write.format("parquet").mode("overwrite").save(target_path)

# Commit the job
job.commit()
