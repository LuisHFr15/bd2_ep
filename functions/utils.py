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
