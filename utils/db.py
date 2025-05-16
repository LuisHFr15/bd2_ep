import boto3
import os
import pymysql
import random
from faker import Faker
from utils.sys_func import get_random_seed
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

def execute_script(script: str, cursor: pymysql.connect.cursor) -> None:
    for command in script.split(';'):
        command = command.strip()
        print("Executando \n", command)
        if command:
            cursor.execute(command)


def create_database(s3: boto3.client, con: pymysql.connect) -> bool:
    cursor = con.cursor()
    bucket_name = os.getenv("BUCKET_NAME")
    try:
        item = s3.get_object(Bucket=bucket_name, Key='functions/create_database.sql')
        script = item['Body'].read().decode('utf-8')
        execute_script(script, cursor)
                
        con.commit()
        print('Database criado com sucesso')
        return True
                

    except Exception as e:
        print('Algo de errado aconteceu ao criar o Database', e)
        return False
    
    else:
        return True
    
def get_email(count: int, lista_nome: list) -> str:
    adendo = ['1','2','3','4','5','6','7','8','9','_']
    
    if count % 5 in (0,1):
        codigo_email = '@gmail.com'
    
    elif count % 5 == 2:
        codigo_email = '@outlook.com'

    else:
        codigo_email = '@hotmail.com'

    acrescimo_email = ''
    for i in range(0,3):
        acrescimo_email += random.choice(adendo)

    email_comunicacao = ''.join(lista_nome).lower() + acrescimo_email + codigo_email

    if len(email_comunicacao) > 30:
        tamanho = len(email_comunicacao)
        caractere_inicial = tamanho - 30
        email_comunicacao = email_comunicacao[caractere_inicial::]

    return email_comunicacao

def get_codigo(count: int) -> str:
    codigo = ''
    # cpf
    if count % 3 != 0:
        for i in range(0,11):
            codigo += str(random.randint(0,9))

    #cnpj
    else:
        for i in range(0,15):
            codigo += str(random.randint(0,9))

    return codigo


def gera_pessoa(count: int) -> list:
    fake = Faker('pt_BR')
    name = fake.name()

    name_list = name.split(' ')
    if any(x in name for x in ('Dr.', 'Dra.', 'Sr.', 'Srta.', 'Sra.')):
        name = ' '.join(name_list[1::])
        name_list = name_list[1::]
    
        
    seed = get_random_seed()

    random.seed(seed + count)
    
    codigo = get_codigo(count)
    
    tipo_codigo = 'F' if len(codigo) == 11 else 'J'

    email_comunicacao = get_email(count, name_list)

    log_criacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    pessoa = [codigo, tipo_codigo, 0, email_comunicacao, log_criacao]

    return pessoa


def alimenta_banco_pessoa(num_rows: int, con: pymysql.connect) -> bool:
    script = """INSERT INTO PESSOA 
    (codigoPessoa, tipoCodigo, quantidadeContas, emailComunicacao, logCriacao)
    VALUES
    """
    cursor = con.cursor()
    for count in range(1, (num_rows + 1)):
        pessoa = gera_pessoa(count)
        script += '('
        for coluna in pessoa:
            if coluna != pessoa[2]:
                script += "'" + str(coluna) + "',"
            else:
                script += str(coluna) + ","

        if count != num_rows:
            script += """),
            """
        else:
            script += ');'
    try:
        cursor.execute(script.replace(',)', ')'))
        con.commit()
        print('Database alimentado com sucesso')
        return True
    except Exception as e:
        print('Falha ao alimentar a tabela Pessoa', e)
        return False
