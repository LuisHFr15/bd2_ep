from dotenv import load_dotenv
import boto3
import os
import pymysql

load_dotenv()

def create_database(s3: boto3.client, con: pymysql.connect) -> bool:
    cursor = con.cursor()
    bucket_name = os.getenv("BUCKET_NAME")
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix='models/')
        for page in pages:
            for obj in page['Contents']:
                item = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                script = item['Body'].read().decode('utf-8')
                cursor.execute(script)
                
        con.commit()
        print('Database criado com sucesso')
        return True
                

    except Exception as e:
        print('Algo de errado aconteceu ao criar o Database', e)
        return False
    
    else:
        return True
    

    