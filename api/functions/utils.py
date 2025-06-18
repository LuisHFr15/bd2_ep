import pymysql
import time
import os
import boto3
import json
from dotenv import load_dotenv

load_dotenv

def execute_script(script: str, cursor: pymysql.connect.cursor) -> None:
    for command in script.split(';'):
        command = command.strip()
        print("Executando \n", command)
        if command:
            cursor.execute(command)

def get_random_seed() -> int:
    seed: int = int(time.time())
    return seed 

def executa_batches(cursor: pymysql.connect.cursor, script: str, data: list, batch_size: int) -> None:
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        tentativas = 3

        while tentativas > 0:
            try:
                cursor.executemany(script, batch)
                break

            except pymysql.err.OperationalError as e:
                if e.args[0] == 1213:
                    print('Deadlock detectado, tentando novamente...')
                    time.sleep(1)
                    tentativas -= 1
                    
                else:
                    raise

            except Exception as e:
                print(e)
                break

def conecta_sessao_aws() -> boto3.Session:
    access_key = os.getenv("AWS_ACCESS_KEY")
    secret_key = os.getenv("AWS_ACCESS_KEY_SECRET")
    db_region = 'sa-east-1'
    session = boto3.Session(aws_access_key_id=access_key
                            , aws_secret_access_key=secret_key
                            , region_name=db_region
                            )
    
    return session


def conecta_db(session: boto3.Session) -> pymysql.connect:
    db_endpoint = os.getenv("DB_ENDPOINT")
    db_port = int(os.getenv("DB_PORT"))
    db_secret_key = os.getenv("DB_SECRET_KEY")
    db_name = os.getenv("DB_NAME")
    os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

    secrets_manager = session.client("secretsmanager")
    value_key = secrets_manager.get_secret_value(SecretId=db_secret_key)
    db_login = json.loads(value_key['SecretString'])

    db_user = db_login['username']
    db_password = db_login['password']
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
    
    return con

def formatar_compacto(valor: float) -> str:
    if valor >= 1_000_000_000:
        return f"{valor / 1_000_000_000:.1f}B"
    elif valor >= 1_000_000:
        return f"{valor / 1_000_000:.1f}M"
    elif valor >= 1_000:
        return f"{valor / 1_000:.1f}K"
    else:
        return f"{valor:.0f}"

