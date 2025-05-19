import sqlite3
from faker import Faker
import random
from datetime import datetime

fake = Faker('pt_BR')

con = sqlite3.connect('.storage/database.db')
cursor = con.cursor()

# Pegue todos os códigos de pessoa e suas quantidades de contas permitidas
cursor.execute("SELECT codigoPessoa, quantidadeContas FROM PESSOA")
pessoas = cursor.fetchall()

# Crie um dicionário para controlar quantas contas já foram criadas para cada pessoa
contas_por_pessoa = {codigo: 0 for codigo, qtd in pessoas}
limite_por_pessoa = {codigo: qtd for codigo, qtd in pessoas}

contas_a_inserir = []
for codigo, limite in pessoas:
    for _ in range(limite):
        agencia = random.randint(1000, 9999)
        nroConta = random.randint(10000, 99999)
        senhaConta = fake.password(length=8)
        renda = round(random.uniform(1000, 20000), 2)
        perfilCredito = random.choice(['Bom', 'Regular', 'Ruim'])
        ativa = random.choice(['S', 'N'])
        dataCriacao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        contas_a_inserir.append((
            codigo, agencia, nroConta, senhaConta, renda, perfilCredito, ativa, dataCriacao
        ))

cursor.executemany(
    "INSERT INTO CONTA (codigoPessoa, agencia, nroConta, senhaConta, renda, perfilCredito, ativa, dataCriacao) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    contas_a_inserir
)

con.commit()
con.close()
print("Tabela CONTA populada")