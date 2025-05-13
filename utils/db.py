from pathlib import Path
from faker import Faker
from datetime import datetime
import os, sqlite3, random, time
from utils.sys_func import get_random_seed, get_root_folder

def get_database_folder() -> str:
    root_folder = get_root_folder()
    os.chdir(root_folder)
    return Path('.storage').resolve()

def get_generate_database_scripts(path: str, scripts: list) -> None:
    for item in os.listdir(path):
        if '.sql' in item:
            scripts.append(item)
            
def execute_sql_file(file_path: str, cursor) -> bool:
    try:
        with open(file_path, 'r') as sql_file:
            cursor.executescript(sql_file.read())
            
        return True
    
    except:
        return False

def create_database() -> bool:
    try:
        db_folder = get_database_folder()
        os.chdir(db_folder)
        path = os.path.join(db_folder, 'models')
        scripts = []
        get_generate_database_scripts(path, scripts)
        
        
        con = sqlite3.connect('database.db')
        cursor = con.cursor()
        
        for script in scripts:
            script_path = os.path.join(db_folder, ('models\\' + script))
            if execute_sql_file(script_path, cursor):
                con.commit()
            
        con.close()
            
        
    except:
        return False
    
    else:
        return True
    
def alimenta_db_pessoa(rows: int) -> bool:
    try:
        db_folder = get_database_folder()
        os.chdir(db_folder)
        con = sqlite3.connect('database.db')
        cursor = con.cursor()
        
        for count in range(1, rows + 1):
            fake = Faker('pt_BR')
            nome = fake.name()
            if nome.startswith(('Dr.', 'Dra.')):
                nome = ' '.join(nome.split(' ')[1:])
                
            seed = get_random_seed() + count
            random.seed(seed)
            
            codigo = ''
            if count % 3 == 0:
                for number in range(0,14):
                    codigo += str(random.randint(0,9))
            else:
                for number in range(0,11):
                    codigo += str(random.randint(0,9))
                    
                
            if len(codigo) == 11:
                tipo_codigo = 'F'
            else :
                tipo_codigo = 'J'
            
            quantidade_contas = random.randint(0, 10)
            
            name_list = nome.split(' ')
            email = name_list[0] + name_list[1] + '@gmail.com'
            
            criacao = datetime.fromtimestamp(time.time())
            log_criacao = str(criacao)
            pessoa = [codigo, tipo_codigo, quantidade_contas, email, log_criacao]
            
            insert_command = 'INSERT INTO PESSOA (codigoPessoa, tipoCodigo, quantidadeContas, emailComunicacao, logCriacao) VALUES (?, ?, ?, ?, ?)'
            
            cursor.execute(insert_command, pessoa)
            con.commit()
            print(f'Linha inserida {count}/{rows}')
            
        return True
            
    except Exception as e:
        print(e)
        return False
    
    