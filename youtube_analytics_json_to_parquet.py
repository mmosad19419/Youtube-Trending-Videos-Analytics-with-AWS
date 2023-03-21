import awswrangler as wr
import pandas as pd
import urllib.parse
import os

"""
Create the lambda function to be deployed to AWS Lambda service
"""

# declare variables using environment variables
osInputS3CleanedLayer = os.environ["s3_cleansed_layer"]
osInputGlueCatalogDataBaseName = os.environ["glue_catalog_db_name"]
osInputGlueCatalogTableName = os.environ["glue_catalog_table_name"]
osInputWriteDataOperation = os.environ["write_data_operation"]

def lambda_handler(event, context):
    # get the ob ject from the bucket and show its content type
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        # creating df from content
        dfRaw = wr.s3.read_json("s3://{}/{}".format(bucket, key))
        
        # extract item column
        dfItem = pd.json_normalize(dfRaw["items"])
        
        # write to s3 in parquet format
        s3WriteObject = wr.s3.to_parquet(
            df = dfItem,
            path = osInputS3CleanedLayer,
            dataset = True,
            database = osInputGlueCatalogDataBaseName,
            table = osInputGlueCatalogTableName,
            mode = osInputWriteDataOperation
        )
        
        return s3WriteObject
         
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e