import pymysql
import os
import boto3
from functions.conta import alimenta_cartao_threaded, alimenta_transacao_threaded
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
        'connect_timeout': 120,
        'read_timeout': 600,
        'write_timeout': 240
    }
    con = pymysql.connect(**con_params)
    # create_database(s3, con)
    cursor = con.cursor()

    print('Executando query')
    cursor.execute("""SELECT c.idConta, c.perfilCredito, CONCAT(pf.primeiroNome, ' ', pf.segundoNome) as nome
    FROM CONTA As c INNER JOIN PESSOAFISICA AS pf
    ON c.codigoPessoa = pf.cpf
    WHERE c.ativa = 'S'""")
    query = cursor.fetchall()
    print('Query realizada, criando lista para criação')
    contas = [[row[0], row[1], row[2]] for row in query]

    print('Alimentando Cartões de Crédito')
    for i in range(22):
        print(f'Tentativa {i + 1}')
        alimenta_cartao_threaded(50000, con_params, contas, num_threads=5, batch_size=500)


    cursor.execute("SELECT DISTINCT idConta FROM CONTACORRENTE")
    query = cursor.fetchall()
    contas_correntes = [id_conta for id_conta in query]

    cursor.execute("SELECT idConta, idCartao FROM CARTAOCREDITO")
    query = cursor.fetchall()
    contas_cartao = [row for row in query]

    print('Alimentando Transacoes')
    for i in range(50):
        print(f'Tentativa {i + 1}')
        alimenta_transacao_threaded(50000, con_params, contas_correntes, contas_cartao, num_threads=5, batch_size=500)



except Exception as e:
    print(e)