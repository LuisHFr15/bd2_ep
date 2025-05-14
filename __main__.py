import pymysql
import os
import boto3
from dotenv import load_dotenv
from utils.db import create_database

load_dotenv()

access_key = os.getenv("AWS_ACCESS_KEY")
secret_key = os.getenv("AWS_ACCESS_KEY_SECRET")
db_endpoint = os.getenv("DB_ENDPOINT")
db_port = int(os.getenv("DB_PORT"))
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_region = 'sa-east-1'
db_name = os.getenv("DB_NAME")
bucket_name = os.getenv("BUCKET_NAME")
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

session = boto3.Session(profile_name='default',
                        region_name='sa-east-1')
rds = session.client("rds")
s3 = session.client("s3"
                    ,aws_access_key_id=access_key
                    ,aws_secret_access_key=secret_key
                    ,region_name=db_region)



token = rds.generate_db_auth_token(DBHostname=db_endpoint
                                ,Port=db_port
                                ,DBUsername=db_user
                                ,Region=db_region)


try:
    con = pymysql.connect(auth_plugin_map={'mysql_clear_password': None}
                        ,host=db_endpoint
                        ,user=db_user
                        ,password=db_password
                        ,port=db_port
                        ,database=db_name)
    
    create_database(s3, con)

    # item = s3.get_object(Bucket=bucket_name, Key='functions/create_database.sql')
    # with open('create_database.sql', 'w') as file:
    #     script = item['Body'].read().decode('utf-8')
    #     file.write(script)
    

except Exception as e:
    print(e)