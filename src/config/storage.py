import boto3
from botocore.client import Config
from typing import Optional

import io
import pandas as pd
from datetime import datetime,date

class MinIO_S3_client:
    s3 = boto3.client('s3',
                    endpoint_url='http://minio:9000',
                    aws_access_key_id='khanhlq10',
                    aws_secret_access_key='khanhlq10',
                    config=Config(signature_version='s3v4')
                    )

class minio_local:
    s3 = boto3.client('s3',
                    endpoint_url='http://localhost:9000',
                    aws_access_key_id='khanhlq10',
                    aws_secret_access_key='khanhlq10',
                    config=Config(signature_version='s3v4')
                    )
def upload_df_to_s3(df:pd.DataFrame,extract_date: Optional[date] = None):
    current_day = extract_date.strftime("%Y_%m_%d")
    current_month = extract_date.strftime("%Y_%m")
    current_year = extract_date.strftime("%Y")

    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer,index=False)
        response = MinIO_S3_client.s3.put_object(Bucket='stock'
                                                 ,Key = f"bronze/stock_price_data/{current_year}/{current_month}/{current_day}.csv"
                                                 ,Body=csv_buffer.getvalue())

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful.")
        else:
            print(f"Unsuccessful.")
