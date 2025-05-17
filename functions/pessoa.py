import boto3
import os
import pymysql
import random
import threading
from tqdm import tqdm
from functions.utils import execute_script
from faker import Faker
from functions.utils import get_random_seed, executa_batches
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()


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
    

def get_email(count: int, lista_nome: list, random: random.Random) -> str:
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


def get_codigo(count: int, random: random.Random) -> str:
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


def gera_pessoa(contador: int, thread_id: int) -> list:
    fake = Faker('pt_BR')
    name = fake.name()

    name_list = name.split(' ')
    if any(x in name for x in ('Dr.', 'Dra.', 'Sr.', 'Srta.', 'Sra.')):
        name = ' '.join(name_list[1::])
        name_list = name_list[1::]
    
        
    seed = get_random_seed()

    rand = random.Random((seed * 1000) + (thread_id * 1000) + (contador * 3))

    
    codigo = get_codigo(contador, rand)
    
    tipo_codigo = 'F' if len(codigo) == 11 else 'J'

    email_comunicacao = get_email(contador, name_list, rand)

    log_criacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    pessoa = [codigo, tipo_codigo, 0, email_comunicacao, log_criacao]
    if tipo_codigo == 'F':
        dados_adicionais = [codigo
                            , name_list[0]
                            , ' '.join(name_list[1::])
                            , fake.date_of_birth(minimum_age=18, maximum_age=82)]
    else:
        dados_adicionais = [codigo
                            , fake.company()
                            , fake.date_of_birth(minimum_age=1, maximum_age=48)]

    return [pessoa, dados_adicionais]


def worker_alimenta_pessoa(con_params: dict, start: int, end: int, thread_id: int, batch_size: int) -> None:
    try:
        con = pymysql.connect(**con_params)
        cursor = con.cursor()

        progress_bar = tqdm(total=(end - start), desc=f"Thread {thread_id}", position=thread_id)

        pessoas = []
        pessoas_juridicas = []
        pessoas_fisicas = []

        for count in range(start, end):
            pessoa = gera_pessoa(count, thread_id)
            pessoas_fisicas.append(pessoa[1]) if pessoa[0][1] == 'F' else pessoas_juridicas.append(pessoa[1])
            pessoas.append(pessoa[0])
            progress_bar.update(1)

        script_pessoa = """INSERT IGNORE INTO PESSOA 
            (codigoPessoa, tipoCodigo, quantidadeContas, emailComunicacao, logCriacao)
            VALUES (%s, %s, %s, %s, %s)
        """
        script_pessoa_juridica = """INSERT IGNORE INTO PESSOAJURIDICA
            (cnpj, razaoSocial, dataFundacao)
            VALUES (%s, %s, %s)
        """
        script_pessoa_fisica = """INSERT IGNORE INTO PESSOAFISICA
            (cpf, primeiroNome, segundoNome, dataNascimento)
            VALUES (%s, %s, %s, %s)
        """

        executa_batches(cursor, script_pessoa, pessoas, batch_size)
        executa_batches(cursor, script_pessoa_fisica, pessoas_fisicas, batch_size)
        executa_batches(cursor, script_pessoa_juridica, pessoas_juridicas, batch_size)

        con.commit()
    except Exception as e:
        print(f'Erro na thread {start}-{end}:', e)
    finally:
        con.close()
        progress_bar.close()
    

def alimenta_banco_pessoa_threaded(num_rows: int, con_params: dict, num_threads: int = 5, batch_size: int = 5000) -> None:
    threads = []
    chunk_size = num_rows // num_threads

    for i in range(num_threads):
        start = i * chunk_size + 1

        end = (i + 1) * chunk_size + 1 if i != num_threads - 1 else num_rows + 1
        t = threading.Thread(target=worker_alimenta_pessoa, args=(con_params, start, end, i, batch_size))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print('Todas as threads finalizaram.')
