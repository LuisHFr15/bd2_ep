import pymysql
import os
import boto3
import time
from functions.pessoa import alimenta_banco_pessoa_threaded
from functions.conta import alimenta_banco_conta_threaded
from dotenv import load_dotenv

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
    con_params = {
        'host': db_endpoint,
        'user': db_user,
        'password': db_password,
        'port': db_port,
        'database': db_name,
        'auth_plugin_map': {'mysql_clear_password': None},
        'connect_timeout': 30,
        'read_timeout': 120,
        'write_timeout': 120
    }
    con = pymysql.connect(**con_params)
    # create_database(s3, con)

    cursor = con.cursor()
    cursor.execute("SELECT COUNT(*) FROM CONTAINVESTIMENTO")
    lista = cursor.fetchall()
    print(lista)
      


except Exception as e:
    print(e)