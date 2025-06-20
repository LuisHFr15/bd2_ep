import boto3, pymysql
from functions.utils import conecta_db

def listar_todas_transacoes(connection: pymysql.connect, offset: int = 0) -> list:
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

def listar_transacoes_filtradas(connection: pymysql.connect
                                ,conta_origem: str
                                ,offset: int = 0) -> list:
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

def listar_todas_contas(connection, offset=0, limit=50):
    cursor = connection.cursor()
    query = """
        SELECT
            c.idConta,
            CASE
                WHEN cc.idConta IS NOT NULL THEN 'CORRENTE'
                WHEN ci.idConta IS NOT NULL THEN 'INVESTIMENTO'
                ELSE 'DESCONHECIDO'
            END AS tipoConta,
            c.codigoPessoa,
            p.tipoCodigo AS tipoPessoa,
            COALESCE(cc.saldoConta, ci.saldoCINvest, 0) AS saldo,
            c.renda AS renda,
            c.perfilCredito,
            c.ativa,
            DATE_FORMAT(c.dataCriacao, '%%d/%%m/%%Y') AS dataAbertura,
            (
                SELECT COUNT(*)
                FROM TRANSACOES t
                WHERE t.contaOrigem = c.idConta
                  AND t.dataTransacao >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            ) AS tx_mes
        FROM CONTA c
        LEFT JOIN CONTACORRENTE cc ON cc.idConta = c.idConta
        LEFT JOIN CONTAINVESTIMENTO ci ON ci.idConta = c.idConta
        LEFT JOIN PESSOA p ON p.codigoPessoa = c.codigoPessoa
        ORDER BY c.idConta
        LIMIT %s OFFSET %s
    """
    cursor.execute(query, (limit, offset))
    cols = ['idConta','tipoConta','codigoPessoa','tipoPessoa','saldo','renda',
            'perfilCredito','ativa','dataAbertura','tx_mes']
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def listar_contas_filtrada(connection, conta, offset=0, limit=50):
    cursor = connection.cursor()
    query = """
        SELECT
            c.idConta,
            CASE WHEN cc.idConta IS NOT NULL THEN 'CORRENTE'
                 WHEN ci.idConta IS NOT NULL THEN 'INVESTIMENTO'
                 ELSE 'DESCONHECIDO' END AS tipoConta,
            c.codigoPessoa,
            p.tipoCodigo AS tipoPessoa,
            COALESCE(cc.saldoConta, ci.saldoCINvest, 0) AS saldo,
            c.renda AS renda,
            c.perfilCredito,
            c.ativa,
            DATE_FORMAT(c.dataCriacao, '%%d/%%m/%%Y') AS dataAbertura,
            (
                SELECT COUNT(*)
                FROM TRANSACOES t
                WHERE t.contaOrigem = c.idConta
                  AND t.dataTransacao >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            ) AS tx_mes
        FROM CONTA c
        LEFT JOIN CONTACORRENTE cc ON cc.idConta = c.idConta
        LEFT JOIN CONTAINVESTIMENTO ci ON ci.idConta = c.idConta
        LEFT JOIN PESSOA p ON p.codigoPessoa = c.codigoPessoa
        WHERE c.idConta = %s
        ORDER BY c.idConta
        LIMIT %s OFFSET %s
    """
    cursor.execute(query, (conta, limit, offset))
    cols = ['idConta','tipoConta','codigoPessoa','tipoPessoa','saldo','renda',
            'perfilCredito','ativa','dataAbertura','tx_mes']
    return [dict(zip(cols, row)) for row in cursor.fetchall()]
  
def levantamento_geral_de_investimentos(connection: pymysql.connect) -> dict:
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
    'total_contas': int(total_contas_investindo[0][0] / 1000),
    'total_pessoas': int(total_pessoas_investindo[0][0] / 13)
  }
  return levantamento_geral

def contar_tipos_contas(connection: pymysql.connect) -> tuple:
    cursor = connection.cursor()
    query = """
        SELECT
            SUM(CASE WHEN cc.idConta IS NOT NULL THEN 1 ELSE 0 END) AS corrente,
            SUM(CASE WHEN ci.idConta IS NOT NULL THEN 1 ELSE 0 END) AS investimento
        FROM CONTA c
        LEFT JOIN CONTACORRENTE cc ON c.idConta = cc.idConta
        LEFT JOIN CONTAINVESTIMENTO ci ON c.idConta = ci.idConta
    """
    cursor.execute(query)
    resultado = cursor.fetchone()
    return resultado[0], resultado[1]

def rendimento_por_mes(connection: pymysql.connect,
                       tipo_investimento: str = 'AMBOS') -> tuple:
    cursor = connection.cursor()
    
    if tipo_investimento not in ('FIXA', 'VARIAVEL'):
      query = """
          SELECT 
            DATE_FORMAT(dataInvest, '%Y-%m') AS mes,
            SUM(oi.quantiaInvest) AS total_investido,
            AVG(i.rendMedioMes) AS media_rendimento
        FROM ORDEMINVESTIMENTO oi
        JOIN INVESTIMENTO i ON oi.idInvest = i.idInvest
        WHERE dataInvest >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
        GROUP BY mes
        ORDER BY mes ASC
      """
      cursor.execute(query)
    else:
      query = """
          SELECT 
            DATE_FORMAT(dataInvest, '%%Y-%%m') AS mes,
            SUM(oi.quantiaInvest) AS total_investido,
            AVG(i.rendMedioMes) AS media_rendimento
        FROM ORDEMINVESTIMENTO oi
        JOIN INVESTIMENTO i ON oi.idInvest = i.idInvest
        WHERE dataInvest >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
          AND i.tipoRenda = %s
        GROUP BY mes
        ORDER BY mes ASC
      """
      cursor.execute(query, tipo_investimento)
      
    resultados = cursor.fetchall()

    datas = []
    rendimentos_acumulados = []
    valor_acumulado = 0.0

    for row in resultados:
        mes, investido_no_mes, rendimento_mensal = row
        valor_acumulado *= (1 + (rendimento_mensal / 100))
        valor_acumulado += (investido_no_mes / 1000)
        datas.append(mes)
        rendimentos_acumulados.append(round(valor_acumulado, 2))

    return datas, rendimentos_acumulados

def ultimas_ordens(connection: pymysql.connect) -> list:
    cursor = connection.cursor()
    query = """
        SELECT idConta, idInvest, quantiaInvest, dataInvest
        FROM ORDEMINVESTIMENTO
        ORDER BY dataInvest DESC
        LIMIT 10
    """
    cursor.execute(query)
    dados = cursor.fetchall()
    return [{
        'conta': row[0],
        'investimento': row[1],
        'quantia': row[2],
        'data': row[3]
    } for row in dados]

def indicadores_contas(connection):
    cursor = connection.cursor()
    query = """
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN ativa='S' THEN 1 ELSE 0 END) AS ativas,
          SUM(CASE WHEN ativa='N' THEN 1 ELSE 0 END) AS inativas,
          SUM(COALESCE(cc.saldoConta, ci.saldoCINvest, 0)) AS saldo_total
        FROM CONTA c
        LEFT JOIN CONTACORRENTE cc ON cc.idConta = c.idConta
        LEFT JOIN CONTAINVESTIMENTO ci ON ci.idConta = c.idConta
    """
    cursor.execute(query)
    total, ativas, inativas, saldo_total = cursor.fetchone()
    return {
        'total_contas': total,
        'contas_ativas': ativas,
        'contas_inativas': inativas,
        'saldo_total': round(saldo_total or 0, 2)
    }

def distribuicao_tipos(connection):
    cursor = connection.cursor()
    query = """
        SELECT tipoConta, COUNT(*)
        FROM (
          SELECT c.idConta,
                 CASE
                   WHEN cc.idConta IS NOT NULL THEN 'Corrente'
                   WHEN ci.idConta IS NOT NULL THEN 'Investimento'
                   ELSE 'Desconhecido'
                 END AS tipoConta
          FROM CONTA c
          LEFT JOIN CONTACORRENTE cc ON cc.idConta = c.idConta
          LEFT JOIN CONTAINVESTIMENTO ci ON ci.idConta = c.idConta
        ) sub
        GROUP BY tipoConta
    """
    cursor.execute(query)
    labels, values = zip(*cursor.fetchall()) if cursor.rowcount else ([], [])
    return {'labels': list(labels), 'values': list(values)}

def contas_ativas_inativas(connection):
    cursor = connection.cursor()
    query = """
        SELECT CASE WHEN ativa='S' THEN 'Ativas' ELSE 'Inativas' END AS status,
               COUNT(*)
        FROM CONTA
        GROUP BY ativa
    """
    cursor.execute(query)
    labels, values = zip(*cursor.fetchall()) if cursor.rowcount else ([], [])
    return {'labels': list(labels), 'values': list(values)}

def tx_por_mes(connection, id_conta):
    cursor = connection.cursor()
    query = """
        SELECT COUNT(*)
        FROM TRANSACOES
        WHERE contaOrigem = %s
          AND dataTransacao >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    """
    cursor.execute(query, (id_conta,))
    return cursor.fetchone()[0] or 0
  
