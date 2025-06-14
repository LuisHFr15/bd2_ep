import pymysql
import threading
import random
from faker import Faker
from functions.utils import get_random_seed, executa_batches
from tqdm import tqdm

def gera_investimento(contador: int, cnpj_oferecedor: str, random: random.Random) -> list:
    id_investimento = random.randint(1, 99999999)
    dias_cotizacao = random.choice([1,2,3,5,10,15,20,25,30])
    dias_retirada = random.choice([1,2,3,5,10,15,20,25,30,35,40,45,50])

    total_investido = round(random.uniform(10000, 3000000), 2)
    tipo_renda = 'FIXA' if contador % 3 != 0 else 'VARIAVEL'
    rend_medio_mes = round(random.uniform(8, 13), 2) if tipo_renda == 'FIXA' else round(random.uniform(4, 30), 2)
    total_contas = random.randint(0, 150)
    ativo = 'S' if contador % 5 != 0 else 'N'

    return [id_investimento, cnpj_oferecedor, dias_cotizacao, dias_retirada, total_investido, tipo_renda, rend_medio_mes, total_contas, ativo]


def gera_ordem_investimento(id_investimento: int, id_conta: int, perfil_credito: str, random: random.Random) -> list:
    fake = Faker('pt_BR')

    id_ordem = random.randint(1, 9999999)
    
    quantias_baixas = [valor for valor in range(100, 1000, 100)]
    quantias_medias = [valor for valor in range(500, 3000, 100)]
    quantias_altas = [valor for valor in range(2000, 20000, 200)]

    if perfil_credito == 'BAIXO':
        quantia_investida = random.choice(quantias_baixas)
    elif perfil_credito == 'MEDIO':
        quantia_investida = random.choice(quantias_medias)
    else:
        quantia_investida = random.choice(quantias_altas)

    data_ordem = fake.date_of_birth(minimum_age=0, maximum_age=5)

    return [id_ordem, id_conta, id_investimento, quantia_investida, data_ordem]


def worker_alimenta_investimento(con_params: dict, start: int, end: int, thread_id: int, batch_size: int, codigo_cnpjs: list) -> None:
    script_investimento = """INSERT IGNORE INTO INVESTIMENTO
                            (idInvest, cnpjOferecedor, diasCotizacao, diasRetirada, totalInvestido, tipoRenda, rendMedioMes, totalContas, ativo)
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

        investimentos = []

        for count in range(start, end):
            seed = get_random_seed()
            rand = random.Random((seed * 1000) + (thread_id * 1000) + (count * thread_id) ** 2)

            cnpj = random.choice(codigo_cnpjs)
            investimento = gera_investimento(count, cnpj, rand)
            investimentos.append(investimento)

            progress_bar.update(1)

        executa_batches(cursor, script_investimento, investimentos, batch_size)
        con.commit()

        print(f'Thread {thread_id} finalizada com sucesso')

    except Exception as e:
        print(f'Falha ao executar thread {thread_id}:', e)

    finally:
        con.close()
        progress_bar.close()


def worker_alimenta_ordens(con_params: dict, start: int, end: int, thread_id: int, batch_size: int, contas: list, investimentos: list) -> None:
    script_ordem = """INSERT IGNORE INTO ORDEMINVESTIMENTO
                            (idOrdem, idConta, idInvest, quantiaInvest, dataInvest)
                            VALUES (%s, %s, %s, %s, %s)"""
    
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

        ordens = []

        for count in range(start, end):
            seed = get_random_seed()
            rand = random.Random((seed * 1000) + (thread_id * 1000) + (count * thread_id) ** 2)

            conta = random.choice(contas)
            investimento = random.choice(investimentos)
            ordem_investimento = gera_ordem_investimento(investimento, conta[0], conta[1], rand)
            ordens.append(ordem_investimento)

            progress_bar.update(1)

        executa_batches(cursor, script_ordem, ordens, batch_size)
        con.commit()

        print(f'Thread {thread_id} finalizada com sucesso')

    except Exception as e:
        print(f'Falha ao executar thread {thread_id}:', e)

    finally:
        con.close()
        progress_bar.close()


def alimenta_banco_investimento_threaded(num_rows: int, con_params: dict, codigos_cnpjs: list, num_threads: int = 5, batch_size: int = 5000) -> None:
    threads = []
    chunk_size = num_rows // num_threads

    for index in range(num_threads):
        start = index * chunk_size + index
        end = (index + 1) * chunk_size + 1 if index != num_threads - 1 else num_rows + 1

        thread = threading.Thread(target=worker_alimenta_investimento, args=(con_params, start, end, index, batch_size, codigos_cnpjs))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print('Todas as threads finalizadas')


def alimenta_banco_ordens_threaded(num_rows: int, con_params: dict, contas: list, investimentos: list, num_threads: int = 5, batch_size: int = 5000) -> None:
    threads = []
    chunk_size = num_rows // num_threads

    for index in range(num_threads):
        start = index * chunk_size + index
        end = (index + 1) * chunk_size + 1 if index != num_threads - 1 else num_rows + 1

        thread = threading.Thread(target=worker_alimenta_ordens, args=(con_params, start, end, index, batch_size, contas, investimentos))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print('Todas as threads finalizadas')