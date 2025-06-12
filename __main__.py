import pymysql
import os
import boto3
import json
from functions.conta import alimenta_cartao_threaded, alimenta_transacao_threaded, alimenta_banco_conta_threaded
from functions.pessoa import alimenta_banco_pessoa_threaded
from functions.investimentos import alimenta_banco_investimento_threaded, alimenta_banco_ordens_threaded
from dotenv import load_dotenv

load_dotenv()

access_key = os.getenv("AWS_ACCESS_KEY")
secret_key = os.getenv("AWS_ACCESS_KEY_SECRET")
db_endpoint = os.getenv("DB_ENDPOINT")
db_port = int(os.getenv("DB_PORT"))
db_secret_key = os.getenv("DB_SECRET_KEY")
db_region = 'sa-east-1'
db_name = os.getenv("DB_NAME")
bucket_name = os.getenv("BUCKET_NAME")
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

session = boto3.Session(aws_access_key_id=access_key
                        , aws_secret_access_key=secret_key
                        , region_name=db_region
                        )
rds = session.client("rds")
s3 = session.client("s3")
secrets_manager = session.client("secretsmanager")
value_key = secrets_manager.get_secret_value(SecretId=db_secret_key)
db_login = json.loads(value_key['SecretString'])

db_user = db_login['username']
db_password = db_login['password']

token = rds.generate_db_auth_token(DBHostname=db_endpoint
                                ,Port=db_port
                                ,DBUsername=db_user
                                ,Region=db_region)


try:
    con_params = {
        'host': db_endpoint,
        'port': db_port,
        'user': db_user,
        'password': db_password,
        'database': db_name,
        'auth_plugin_map': {'mysql_clear_password': None},
        'connect_timeout': 30,
        'read_timeout': 240,
        'write_timeout': 240
    }
    con = pymysql.connect(**con_params)
    # create_database(s3, con)
    cursor = con.cursor()

    cursor.execute("SELECT COUNT(*) FROM TRANSACOES")
    query = cursor.fetchall()
    print(query[0])
    pass

except Exception as e:
    print(e)