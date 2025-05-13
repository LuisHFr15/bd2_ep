import pymysql
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

aws_profile = os.getenv("AWS_PROFILE")
db_endpoint = os.getenv("DB_ENDPOINT")
db_port = int(os.getenv("DB_PORT"))
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_region = 'sa-east-1'
db_name = os.getenv("DB_NAME")
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

session = boto3.Session(profile_name=aws_profile,
                        aws_access_key_id=aws_profile,
                        aws_secret_access_key=aws_profile,
                        region_name='sa-east-1')
rds = session.client("rds")

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
    
    cursor = con.cursor()
    cursor.execute("""SELECT now()""")
    query = cursor.fetchall()
    print(query)
except Exception as e:
    print(e)