import json
import pandas as pd
import urllib3
import boto3
import os
import logging
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
s3_key_raw = "raw_data/economicIndicators.json"
s3_key_clean = "transformeddata/transformed.csv"

http = urllib3.PoolManager()
url = "https://api.census.gov/data/timeseries/eits/resconst?get=cell_value,data_type_code,time_slot_id,error_data,category_code,seasonally_adj&time=2004-05"

conn = psycopg2.connect(
                host=os.environ['host'],
                database=os.environ['dbname'],
                user=os.environ['user'],
                password=os.environ['password']
            )

cur = conn.cursor()

def lambda_handler(event, context):
    try:
        response = http.request('GET', url)
        if response.status == 200:
            data = json.loads(response.data.decode('utf-8'))
            df = pd.DataFrame(data[1:], columns=data[0])
            json_output = df.to_json(orient='records')

            # Writing raw data to raw data bucket
            s3_bucket_raw = "apprentice-training-data-ml-dev-sushan-raw"
            s3.put_object(Bucket=s3_bucket_raw, Key=s3_key_raw, Body=json.dumps(json_output))

            # Applying Transformations
            transformed_1 = df.drop(columns=['time_slot_id'])
            transformed_1.columns = transformed_1.columns.str.upper()
            transformed_2 = transformed_1
            transformed_2['CELL_VALUE'] = transformed_2['CELL_VALUE'].astype(float)
            transformed_3 = transformed_2
            columns_to_convert = ['DATA_TYPE_CODE', 'CATEGORY_CODE']
            for col in columns_to_convert:
                transformed_3[col] = transformed_3[col].str.lower()

            transformed_csv = transformed_3.to_csv(index=False)

            # Writing transformed data to clean data bucket
            s3_bucket_clean = "apprentice-training-data-ml-dev-sushan-cleaned"
            s3.put_object(Bucket=s3_bucket_clean, Key=s3_key_clean, Body=transformed_csv)

            logger.info(msg="Dumped cleaned data")

            for index, row in transformed_3.iterrows():
                cur.execute(
                    "INSERT INTO sushan_testDataTable_etl (CELL_VALUE, DATA_TYPE_CODE, ERROR_DATA, CATEGORY_CODE, SEASONALLY_ADJ, TIME) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        row['CELL_VALUE'],
                        row['DATA_TYPE_CODE'],
                        row['ERROR_DATA'],
                        row['CATEGORY_CODE'],
                        row['SEASONALLY_ADJ'],
                        row['TIME']
                    )
                )

            conn.commit()

    except Exception as e:
        logger.error(msg="Error: " + str(e))
        if conn:
            conn.rollback()

    finally:
        if conn:
            conn.close()

    return {
        'statusCode': 200,
        'body': "Data Uploaded to S3 and RDS"
    }
