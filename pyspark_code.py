import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Define the parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark_session = glue_context.spark_session
glue_job = Job(glue_context)
glue_job.init(args['JOB_NAME'], args)

# Predicate for filtering the regions
region_filter = "region in ('ca', 'gb', 'us')"

# Create a dynamic frame from the Glue catalog with the filter applied
source_data = glue_context.create_dynamic_frame.from_catalog(
    database="db_youtube_raw",
    table_name="raw_statistics",
    transformation_ctx="source_data",
    push_down_predicate=region_filter
)

# Define the mapping for the required fields
field_mapping = [
    ("video_id", "string", "video_id", "string"),
    ("trending_date", "string", "trending_date", "string"),
    ("title", "string", "title", "string"),
    ("channel_title", "string", "channel_title", "string"),
    ("category_id", "long", "category_id", "long"),
    ("publish_time", "string", "publish_time", "string"),
    ("tags", "string", "tags", "string"),
    ("views", "long", "views", "long"),
    ("likes", "long", "likes", "long"),
    ("dislikes", "long", "dislikes", "long"),
    ("comment_count", "long", "comment_count", "long"),
    ("thumbnail_link", "string", "thumbnail_link", "string"),
    ("comments_disabled", "boolean", "comments_disabled", "boolean"),
    ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
    ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
    ("description", "string", "description", "string"),
    ("region", "string", "region", "string")
]

# Apply mapping to the dynamic frame
mapped_data = ApplyMapping.apply(frame=source_data, mappings=field_mapping, transformation_ctx="mapped_data")

# Resolve any potential choice conflicts
resolved_data = ResolveChoice.apply(frame=mapped_data, choice="make_struct", transformation_ctx="resolved_data")

# Drop any null fields from the frame
cleaned_data = DropNullFields.apply(frame=resolved_data, transformation_ctx="cleaned_data")

# Write the cleaned data back to S3 in parquet format, partitioned by region
final_data_frame = cleaned_data.toDF().coalesce(1)
final_dynamic_frame = DynamicFrame.fromDF(final_data_frame, glue_context, "final_dynamic_frame")

glue_context.write_dynamic_frame.from_options(
    frame=final_dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": "s3://bigdata-on-youtube-cleansed-euwest1-14317621-dev/youtube/raw_statistics/",
        "partitionKeys": ["region"]
    },
    format="parquet",
    transformation_ctx="datasink"
)

# Commit the job
glue_job.commit()
