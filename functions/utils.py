import pymysql
import time

def execute_script(script: str, cursor: pymysql.connect.cursor) -> None:
    for command in script.split(';'):
        command = command.strip()
        print("Executando \n", command)
        if command:
            cursor.execute(command)

def get_random_seed() -> int:
    seed: int = int(time.time())
    return seed 

def executa_batches(cursor: pymysql.connect.cursor, script: str, dados: list, batch_size: int) -> None:
    for index in range(0, len(dados), batch_size):
        batch = dados[index:(index + batch_size)]
        cursor.executemany(script, batch)