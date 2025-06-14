import pymysql
import random
import threading
from datetime import datetime, date
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
    nro_conta = rand.randint(1, 999999999)
    senha_conta = fake.password()


    if contador % 5 in (0, 1):
        perfil_credito = 'BAIXO'
        renda = rand.randint(900, 3000)
    elif contador % 5 in (2,3):
        perfil_credito = 'MEDIO'
        renda = rand.randint(3000, 17000)
    else:
        perfil_credito = 'ALTO'
        renda = rand.randint(17000, 90000)

    ativa = 'S' if contador % 10 == 0 else 'N'
    data_criacao = datetime.now().strftime('%Y-%m-%d')

    conta = [codigoPessoa, agencia, nro_conta, senha_conta, renda, perfil_credito, ativa, data_criacao]

    if tipo_conta == 'CORRENTE':
        conta_corrente = gera_conta_corrente(contador, rand, perfil_credito)
        return [conta, tipo_conta, conta_corrente]

    else:
        conta_investimento = gera_conta_investimento(contador, rand, perfil_credito)
        return [conta, tipo_conta, conta_investimento]


def gera_cartao_credito(contador: int, id_conta: int, perfil_credito: str, nome_pessoa: str, random: random.Random) -> list:
    id_cartao = random.randint(1, 999999999)

    nro_cartao = ''
    for i in range(16):
        nro_cartao += str(random.randint(0, 9))

    cvc = random.randint(100, 999)
    limite_baixo = [limite for limite in range(1000, 4000, 100)]
    limite_medio = [limite for limite in range(4000, 10000, 100)]
    limite_alto = [limite for limite in range(10000, 50000, 1000)]

    dia = 1
    mes = random.randint(1, 12)
    ano = random.randint(2020, 2035)

    validade = date(ano, mes, dia)
    validade = str(datetime.strftime(validade, '%Y-%m-%d'))

    if contador % 8 in (0, 1) and ano > 25:
        ativo = 'S'
    else:
        ativo = 'N'

    if perfil_credito == 'BAIXO':
        limite = random.choice(limite_baixo)
    elif perfil_credito == 'MEDIO':
        limite = random.choice(limite_medio)
    else:
        limite = random.choice(limite_alto)



    fatura = round(random.uniform(100, 4000), 2) if ativo == 'S' else 0

    return [id_cartao, id_conta, nro_cartao, cvc, validade, nome_pessoa, limite, fatura, ativo]


def gera_transacao(contador: int, conta_origem: int, conta_destino: int, id_cartao: int, random: random.Random) -> list:
    fake = Faker('pt_BR')
    valor_transacao = round(random.uniform(1, 6000), 2)

    if valor_transacao > 3000 or contador % 4 in (0, 1) or id_cartao is None:
        tipo_transacao = 'DEBITO'
    else:
        tipo_transacao = 'CREDITO'

    data_transacao = fake.date_of_birth(minimum_age=0, maximum_age=5)
    data_transacao = str(datetime.strftime(data_transacao, '%Y-%m-%d'))
    if tipo_transacao == 'CREDITO':
        transacao = [conta_origem, conta_destino, id_cartao, tipo_transacao, valor_transacao, data_transacao]
    else:
        transacao = [conta_origem, conta_destino, None, tipo_transacao, valor_transacao, data_transacao]

    return transacao

def worker_alimenta_contas(con_params: dict, start: int, end: int, thread_id: int, batch_size: int, codigos_pessoas: list) -> None:
    con = pymysql.connect(**con_params)
    cursor = con.cursor()
    try:
        progress_bar = tqdm(
            total=(end - start),
            desc=f"Thread {thread_id}",
            position=thread_id,
            dynamic_ncols=True,
            leave=False
        )
        contas_correntes = []
        contas_investimento = []

        for count in range(start, end):
            seed = get_random_seed()
            rand = random.Random((seed * 1000) + (thread_id * 1000) + (count * thread_id) ** 2)

            codigo_pessoa = random.choice(codigos_pessoas)
            conta = gera_contas(count, thread_id, codigo_pessoa, rand)

            conta_base = conta[0]  
            tipo = conta[1]        # 'CORRENTE' ou 'INVESTIMENTO'
            dados_tipo = conta[2]  
            
            progress_bar.update(1)

            cursor.execute("""
                INSERT IGNORE INTO CONTA
                (codigoPessoa, agencia, nroConta, senhaConta, renda, perfilCredito, ativa, dataCriacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, conta_base)

            idConta = cursor.lastrowid

            if tipo == 'CORRENTE':
                contas_correntes.append([idConta] + dados_tipo)
            else:
                contas_investimento.append([idConta] + dados_tipo)


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


def worker_alimenta_cartao_credito(con_params: dict, start: int, end: int, thread_id: int, batch_size: int, contas: list) -> None:
    script_cartao = """INSERT IGNORE INTO CARTAOCREDITO
                            (idCartao, idConta, nroCartao, cvc, validade, nomeCartao, limiteCartao, faturaCartao, ativo)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    con = pymysql.connect(**con_params)
    cursor = con.cursor()

    try:
        progress_bar = tqdm(
            total=(end - start),
            desc=f"Thread {thread_id}",
            position=thread_id,
            dynamic_ncols=True,
            leave=False
        )

        cartoes = []

        for count in range(start, end):
            seed = get_random_seed()
            rand = random.Random((seed * 1000) + (thread_id * 1000) + (count * thread_id) ** 2)

            conta = random.choice(contas)
            cartao = gera_cartao_credito(count, conta[0], conta[1], conta[2], rand)
            cartoes.append(cartao)

            progress_bar.update(1)

        executa_batches(cursor, script_cartao, cartoes, batch_size)
        con.commit()

        print(f'Thread {thread_id} finalizada com sucesso')

    except Exception as e:
        print(f'Falha ao executar thread {thread_id}:', e)

    finally:
        con.close()
        progress_bar.close()


def worker_alimenta_transacao(con_params: dict, start: int, end: int, thread_id: int, batch_size: int, contas: list, cartoes: list) -> None:
    script_transacao = """INSERT INTO TRANSACOES
                            (contaOrigem, contaDestino, idCartao, tipoTransacao, valorTransacao, dataTransacao)
                            VALUES (%s, %s, %s, %s, %s, %s)"""
    
    con = pymysql.connect(**con_params)
    cursor = con.cursor()

    try:
        progress_bar = tqdm(
            total=(end - start),
            desc=f"Thread {thread_id}",
            position=thread_id,
            dynamic_ncols=True,
            leave=False
        )

        transacoes = []

        for count in range(start, end):
            seed = get_random_seed()
            rand = random.Random((seed * 1000) + (thread_id * 1000) + (count * thread_id) ** 2)

            conta = random.choice(contas)
            cartao = random.choice(cartoes)
            transacao = gera_transacao(count, cartao[0], conta, cartao[1], rand)
            transacoes.append(transacao)

            progress_bar.update(1)

        executa_batches(cursor, script_transacao, transacoes, batch_size)
        con.commit()

        print(f'Thread {thread_id} finalizada com sucesso')

    except Exception as e:
        print(f'Falha ao executar thread {thread_id}:', e)

    finally:
        con.close()
        progress_bar.close()



def alimenta_banco_conta_threaded(num_rows: int, con_params: dict, codigos_pessoas: list, num_threads: int = 5,  batch_size: int = 5000) -> None:
    threads = []
    chunk_size = num_rows // num_threads

    for i in range(num_threads):
        start = i * chunk_size + i
        end = (i + 1) * chunk_size + 1 if i != num_threads - 1 else num_rows + 1

        thread = threading.Thread(target=worker_alimenta_contas, args=(con_params, start, end, i, batch_size, codigos_pessoas))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print('Todas as threads finalizaram.')


def alimenta_cartao_threaded(num_rows: int, con_params: dict, contas: list, num_threads: int = 5, batch_size: int = 5000) -> None:
    threads = []
    chunk_size = num_rows // num_threads

    for index in range(num_threads):
        start = index * chunk_size + index
        end = (index + 1) * chunk_size + 1 if index != num_threads - 1 else num_rows + 1


        thread = threading.Thread(target=worker_alimenta_cartao_credito, args=(con_params, start, end, index, batch_size, contas))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print('Todas as threads finalizadas')

def alimenta_transacao_threaded(num_rows: int, con_params: dict, contas: list, cartoes: list, num_threads: int = 5, batch_size: int = 5000) -> None:
    threads = []
    chunk_size = num_rows // num_threads

    for index in range(num_threads):
        start = index * chunk_size + index
        end = (index + 1) * chunk_size + 1 if index != num_threads - 1 else num_rows + 1


        thread = threading.Thread(target=worker_alimenta_transacao, args=(con_params, start, end, index, batch_size, contas, cartoes))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print('Todas as threads finalizadas')