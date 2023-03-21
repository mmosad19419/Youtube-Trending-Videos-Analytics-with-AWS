import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsglue import DynamicFrame
import re


def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(
                ctx,
                field.dataType,
                new_path + field.name,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(
                ctx,
                schema.elementType,
                path,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split(".")[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set(
                    [
                        item.strip() if isinstance(item, str) else item
                        for item in distinct_
                    ]
                )
            elif isinstance(distinct_, str):
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif (
            isinstance(schema, IntegerType)
            or isinstance(schema, LongType)
            or isinstance(schema, DoubleType)
        ):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output


def drop_nulls(
    glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx
) -> DynamicFrame:
    nullColumns = _find_null_fields(
        frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame
    )
    return DropFields.apply(
        frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Ingest Data From Source
IngestDataFromSource_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="youtube-analytics-raw",
    push_down_predicate="region in ('ca','gb','us')",
    table_name="raw_statistics",
    transformation_ctx="IngestDataFromSource_node1",
)

# Script generated for node Filter
Filter_node1679226163137 = Filter.apply(
    frame=IngestDataFromSource_node1,
    f=lambda row: (
        bool(re.match("ca", row["region"]))
        or bool(re.match("gb", row["region"]))
        or bool(re.match("us", row["region"]))
    ),
    transformation_ctx="Filter_node1679226163137",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=Filter_node1679226163137,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "bigint"),
        ("publish_time", "string", "publish_time", "timestamp"),
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

# Script generated for node Drop Null Fields
DropNullFields_node1679226098912 = drop_nulls(
    glueContext,
    frame=ApplyMapping_node2,
    nullStringSet={"", "null"},
    nullIntegerSet={-1},
    transformation_ctx="DropNullFields_node1679226098912",
)

# Script generated for node Output Location
OutputLocation_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropNullFields_node1679226098912,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://youtube-analytics-cleansed-us-east-1-dev/youtube/raw_statistics_cleansed/",
        "partitionKeys": ["region"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="OutputLocation_node3",
)

job.commit()
