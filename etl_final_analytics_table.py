import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["analytics_etl"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["analytics_etl"], args)

# Script generated for node raw_statistics_cleansed_reference_data_db_table
raw_statistics_cleansed_reference_data_db_table = glueContext.create_dynamic_frame.from_catalog(
    database="youtube-analytics-cleansed",
    table_name="raw_statistics_cleansed_reference_data",
    transformation_ctx="raw_statistics_cleansed_reference_data_db_table_node1679239308673",
)

# Script generated for node raw_statistics_cleansed_db_table
raw_statistics_cleansed_db_table = (
    glueContext.create_dynamic_frame.from_catalog(
        database="youtube-analytics-cleansed",
        table_name="raw_statistics_cleansed",
        transformation_ctx="raw_statistics_cleansed_db_table_node1679239369855",
    )
)

# Script generated for node Join
Join_node1679239438319 = Join.apply(
    frame1=raw_statistics_cleansed_reference_data_db_table,
    frame2=raw_statistics_cleansed_db_table,
    keys1=["id"],
    keys2=["category_id"],
    transformation_ctx="Join_node1679239438319",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=Join_node1679239438319,
    mappings=[
        ("kind", "string", "kind", "string"),
        ("etag", "string", "etag", "string"),
        ("id", "long", "id", "bigint"),
        ("snippet_channelid", "string", "snippet_channelid", "string"),
        ("snippet_title", "string", "snippet_title", "string"),
        ("snippet_assignable", "boolean", "snippet_assignable", "boolean"),
        ("video_id", "string", "video_id", "bigint"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "bigint"),
        ("publish_time", "timestamp", "publish_time", "timestamp"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "bigint"),
        ("likes", "long", "likes", "bigint"),
        ("dislikes", "long", "dislikes", "bigint"),
        ("comment_count", "long", "comment_count", "bigint"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Output Table
OutputTable_node3 = glueContext.getSink(
    path="s3://youtube-analytics-cleansed-us-east-1-analytics",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "publish_time"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="OutputTable_node3",
)
OutputTable_node3.setCatalogInfo(
    catalogDatabase="youtube-analytics", catalogTableName="youtube_analytics_table"
)
OutputTable_node3.setFormat("glueparquet")
OutputTable_node3.writeFrame(ApplyMapping_node2)
job.commit()
