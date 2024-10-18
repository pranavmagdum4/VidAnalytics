import awswrangler as wr
import pandas as pd
import urllib.parse
import os

# Environment variable names for the S3 path and Glue catalog details
cleansed_layer_path = os.environ['s3_cleansed_layer']
glue_database_name = os.environ['glue_catalog_db_name']
glue_table_name = os.environ['glue_catalog_table_name']
data_write_mode = os.environ['write_data_operation']

def lambda_handler(event, context):
    # Extracting bucket and object key from the incoming event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        # Load the JSON data from S3 into a DataFrame
        raw_data = wr.s3.read_json(f's3://{source_bucket}/{object_key}')

        # Normalize the JSON structure into a flat DataFrame
        normalized_data = pd.json_normalize(raw_data['items'])

        # Save the transformed DataFrame to S3 in Parquet format
        response = wr.s3.to_parquet(
            df=normalized_data,
            path=cleansed_layer_path,
            dataset=True,
            database=glue_database_name,
            table=glue_table_name,
            mode=data_write_mode
        )

        return response
    except Exception as error:
        print(error)
        print(f'Error retrieving object {object_key} from bucket {source_bucket}. '
              'Ensure the object exists and the bucket is in the same region as this function.')
        raise error

