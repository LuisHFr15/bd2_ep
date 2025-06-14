import pymysql

def listar_todas_transacoes(connection: pymysql.connect, conta_origem: str = None
                            ,offset: int = 0) -> list:
  cursor = connection.cursor()
  if conta_origem:
    query = """SELECT idTransacao, contaOrigem, contaDestino
                  , tipoTransacao, valorTransacao, dataTransacao
                  FROM TRANSACOES
                  WHERE contaOrigem = %s
                  LIMIT 50 OFFSET %s"""
    cursor.execute(query, (conta_origem, offset))
    
  elif not conta_origem:
    query = """SELECT idTransacao, contaOrigem, contaDestino
                  , tipoTransacao, valorTransacao, dataTransacao
                  FROM TRANSACOES
                  LIMIT 50 OFFSET %s"""
    cursor.execute(query, offset)
  
  response = cursor.fetchall()
  
  transacoes = []
  for row in response:
    transacoes.append({
      'id': row[0],
      'origem': row[1],
      'destino': row[2],
      'tipo': row[3],
      'valor': row[4],
      'data': row[5]
    })
    
  return transacoes