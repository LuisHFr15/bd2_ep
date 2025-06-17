import boto3
from functions.utils import conecta_db

def listar_todas_transacoes(session: boto3.Session, offset: int = 0) -> list:
  connection = conecta_db(session)
  cursor = connection.cursor()

    
  query = """SELECT idTransacao, contaOrigem, contaDestino
                  , tipoTransacao, valorTransacao, dataTransacao
                  FROM TRANSACOES
                  ORDER BY dataTransacao DESC
                  LIMIT 10 OFFSET %s"""
  cursor.execute(query, offset)
  
  response = cursor.fetchall()
  connection.close()
  
  transacoes = []
  for row in response:
    if len(row) < 6:
      continue
    transacoes.append({
      'id': row[0],
      'origem': row[1],
      'destino': row[2],
      'tipo': row[3],
      'valor': row[4],
      'data': row[5]
    })
    
  return transacoes

def listar_transacoes_filtradas(session: boto3.Session
                                ,conta_origem: str
                                ,offset: int = 0) -> list:
  connection = conecta_db(session)
  cursor = connection.cursor()
  query = """SELECT idTransacao, contaOrigem, contaDestino
                  , tipoTransacao, valorTransacao, dataTransacao
                  FROM TRANSACOES
                  WHERE contaOrigem = %s
                  ORDER BY dataTransacao DESC
                  LIMIT 10 OFFSET %s"""
  cursor.execute(query, (conta_origem, offset))
  
  response = cursor.fetchall()
  connection.close()
  
  transacoes = []
  for row in response:
    if len(row) < 6:
      continue
    transacoes.append({
      'id': row[0],
      'origem': row[1],
      'destino': row[2],
      'tipo': row[3],
      'valor': row[4],
      'data': row[5]
    })
    
  return transacoes

def listar_todas_contas(session: boto3.Session, offset: int = 0) -> list:
  connection = conecta_db(session)
  cursor = connection.cursor()

  query = """SELECT c.idConta
  , CASE WHEN cc.saldoConta IS NULL THEN 'Investimento'
    ELSE 'Corrente'
    END AS tipoConta
  , c.codigoPessoa
  , CASE WHEN p.tipoCodigo = 'J' THEN 'Jurídica'
    ELSE 'Física'
    END AS tipoPessoa
  , CASE WHEN cc.saldoConta IS NULL THEN ci.saldoCINvest
    ELSE cc.saldoConta
    END AS saldoAtual
  , c.renda
  , c.perfilCredito
  , c.ativa
  FROM CONTA AS c LEFT JOIN PESSOA AS p
    ON c.codigoPessoa = p.codigoPessoa
  LEFT JOIN CONTACORRENTE AS cc
    ON c.idConta = cc.idConta
  LEFT JOIN CONTAINVESTIMENTO AS ci
    ON c.idConta = ci.idConta
    
  LIMIT 10 OFFSET %s"""
  cursor.execute(query, offset)
  
  response = cursor.fetchall()
  connection.close()
  contas = []
  for row in response:
    if len(row) < 8:
      continue
    contas.append({
      'idConta': row[0],
      'tipoConta': row[1],
      'codigoPessoa': row[2],
      'tipoPessoa': row[3],
      'saldo': row[4],
      'renda': row[5],
      'perfilCredito': row[6],
      'ativo': row[7]
    })
    
  return contas

def listar_contas_filtrada(session: boto3.Session
                           ,conta: str
                           ,offset: int = 0) -> list:
  connection = conecta_db(session)
  cursor = connection.cursor()

  query = """SELECT c.idConta
  , CASE WHEN cc.saldoConta IS NULL THEN 'Investimento'
    ELSE 'Corrente'
    END AS tipoConta
  , c.codigoPessoa
  , CASE WHEN p.tipoCodigo = 'J' THEN 'Jurídica'
    ELSE 'Física'
    END AS tipoPessoa
  , CASE WHEN cc.saldoConta IS NULL THEN ci.saldoCINvest
    ELSE cc.saldoConta
    END AS saldoAtual
  , c.renda
  , c.perfilCredito
  , c.ativa
  FROM CONTA AS c LEFT JOIN PESSOA AS p
    ON c.codigoPessoa = p.codigoPessoa
  LEFT JOIN CONTACORRENTE AS cc
    ON c.idConta = cc.idConta
  LEFT JOIN CONTAINVESTIMENTO AS ci
    ON c.idConta = ci.idConta
    
  WHERE c.idConta = %s
  LIMIT 10 OFFSET %s"""
  cursor.execute(query, (conta, offset))
  
  response = cursor.fetchall()
  connection.close()
  
  contas = []
  for row in response:
    if len(row) < 8:
      continue
    contas.append({
      'idConta': row[0],
      'tipoConta': row[1],
      'codigoPessoa': row[2],
      'tipoPessoa': row[3],
      'saldo': row[4],
      'renda': row[5],
      'perfilCredito': row[6],
      'ativo': row[7]
    })
    
  return contas

def levantamento_geral_de_investimentos(session: boto3.Session) -> dict:
  connection = conecta_db(session)
  cursor = connection.cursor()
  
  cursor.execute("SELECT SUM(totalInvestido) FROM INVESTIMENTO WHERE ativo = 'S';")
  total_dinheiro_investido = cursor.fetchall()
  
  cursor.execute("SELECT SUM(totalContas) FROM INVESTIMENTO WHERE ativo = 'S';")
  total_contas_investindo = cursor.fetchall()
  
  cursor.execute("""SELECT COUNT(DISTINCT C.codigoPessoa)
                 FROM CONTAINVESTIMENTO AS CI INNER JOIN CONTA AS C
                 ON CI.idConta = C.idConta
                 WHERE dinheiroInvestido > 0""")
  total_pessoas_investindo = cursor.fetchall()
  
  levantamento_geral = {
    'total_investido': int(total_dinheiro_investido[0][0] / 1000),
    'total_contas': total_contas_investindo[0][0],
    'total_pessoas': total_pessoas_investindo[0][0]
  }
  return levantamento_geral