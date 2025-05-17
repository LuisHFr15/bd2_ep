import pymysql
import random
import threading
from datetime import datetime
from faker import Faker
from functions.utils import get_random_seed, executa_batches
from tqdm import tqdm

def gera_conta_corrente(contador: int, random: random.Random, perfilCredito: str) -> list:
    limite_baixo = [value for value in range(100, 500, 100)]
    limite_medio = [value for value in range(500, 1500, 100)]
    limite_alto = [value for value in range(1500, 10000, 500)]

    saldo_baixo = round(random.uniform(0, 1800), 2)
    saldo_medio = round(random.uniform(0, 8000), 2)
    saldo_alto = round(random.uniform(2000, 35000), 2)

    if perfilCredito == 'BAIXO':
        limite = random.choice(limite_baixo)
        saldo = saldo_baixo
    elif perfilCredito == 'MEDIO':
        limite = random.choice(limite_medio)
        saldo = saldo_medio
    else:
        limite = random.choice(limite_alto)
        saldo = saldo_alto

    return [saldo, limite]


def gera_conta_investimento(contador: int, random: random.Random, perfilCredito: str) -> list:
    saldo_baixo = round(random.uniform(0, 1200), 2)
    saldo_medio = round(random.uniform(0, 2500), 2)
    saldo_alto = round(random.uniform(2000, 10000), 2)

    investido_baixo = [value for value in range(100, 10000, 100)]
    investido_medio = [value for value in range(10000, 100000, 100)]
    investido_alto = [value for value in range(100000, 1500000, 500)]

    if perfilCredito == 'BAIXO':
        investido = random.choice(investido_baixo)
        saldo = saldo_baixo
        quantidade = random.randint(1, 4)
    elif perfilCredito == 'MEDIO':
        investido = random.choice(investido_medio)
        saldo = saldo_medio
        quantidade = random.randint(2, 6)
    else:
        investido = random.choice(investido_alto)
        saldo = saldo_alto
        quantidade = random.randint(4, 12)

    return [saldo, investido, quantidade]


def gera_contas(contador: int, thread_id: int, codigoPessoa: str, rand: random.Random) -> list:
    tipo_conta = 'CORRENTE' if contador % 5 in (0,1,2,3) else 'INVESTIMENTO'

    fake = Faker('pt_BR')

    agencia = rand.randint(1, 99999)
    nroConta = rand.randint(1, 999999999)
    senhaConta = fake.password()


    if contador % 5 in (0, 1):
        perfilCredito = 'BAIXO'
        renda = rand.randint(900, 3000)
    elif contador % 5 in (2,3):
        perfilCredito = 'MEDIO'
        renda = rand.randint(3000, 17000)
    else:
        perfilCredito = 'ALTO'
        renda = rand.randint(17000, 90000)

    ativa = 'S' if contador % 10 == 0 else 'N'
    dataCriacao = datetime.now().strftime('%Y-%m-%d')

    conta = [codigoPessoa, agencia, nroConta, senhaConta, renda, perfilCredito, ativa, dataCriacao]

    if tipo_conta == 'CORRENTE':
        conta_corrente = gera_conta_corrente(contador, rand, perfilCredito)
        return [conta, tipo_conta, conta_corrente]

    else:
        conta_investimento = gera_conta_investimento(contador, rand, perfilCredito)
        return [conta, tipo_conta, conta_investimento]

def worker_alimenta_contas(con_params: dict, start: int, end: int, thread_id: int, batch_size: int) -> None:
    try:
        con = pymysql.connect(**con_params)
        cursor = con.cursor()

        progress_bar = tqdm(total=(end - start), desc=f"Thread {thread_id}", position=thread_id)
        cursor.execute("SELECT DISTINCT codigoPessoa FROM PESSOA")
        codigos_pessoas = [row[0] for row in cursor.fetchall()]

        contas_correntes = []
        contas_investimento = []

        for count in range(start, end):
            seed = get_random_seed()
            rand = random.Random((seed * 1000) + (thread_id + 1000) + (count * 3))

            codigoPessoa = random.choice(codigos_pessoas)
            conta = gera_contas(count, thread_id, codigoPessoa, rand)

            conta_base = conta[0]  
            tipo = conta[1]        # 'CORRENTE' ou 'INVESTIMENTO'
            dados_tipo = conta[2]  

            cursor.execute("""
                INSERT INTO IGNORE CONTA
                (codigoPessoa, agencia, nroConta, senhaConta, rendaMensal, perfilCredito, ativa, dataCriacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, conta_base)

            idConta = cursor.lastrowid

            if tipo == 'CORRENTE':
                contas_correntes.append([idConta] + dados_tipo)
            else:
                contas_investimento.append([idConta] + dados_tipo)

            progress_bar.update(1)

        # Scripts de insert
        script_conta_corrente = """INSERT IGNORE INTO CONTACORRENTE
        (idConta, saldoConta, limiteConta) VALUES (%s, %s, %s)"""

        script_conta_investimento = """INSERT IGNORE INTO CONTAINVESTIMENTO
        (idConta, saldoCINvest, dinheiroInvestido, quantidadeInvest)
        VALUES (%s, %s, %s, %s)"""

        executa_batches(cursor, script_conta_corrente, contas_correntes, batch_size)
        executa_batches(cursor, script_conta_investimento, contas_investimento, batch_size)
        con.commit()

    except Exception as e:
        print(f'Falha ao executar thread {start}-{end}: {e}')

    finally:
        con.close()
        progress_bar.close()


def alimenta_banco_conta_threaded(num_rows: int, con_params: dict, num_threads: int = 5, batch_size: int = 5000) -> None:
    threads = []
    chunk_size = num_rows // num_threads

    for i in range(num_threads):
        start = i * chunk_size + i

        end = (i + 1) * chunk_size + 1 if i != num_threads - 1 else num_rows + 1
        thread = threading.Thread(target=worker_alimenta_contas, args=(con_params, start, end, i, batch_size))
        thread.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print('Todas as threads finalizaram.')