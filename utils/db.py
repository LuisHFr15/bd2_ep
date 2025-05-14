from dotenv import load_dotenv
import boto3
import os
import pymysql

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
    

    