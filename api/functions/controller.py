import boto3
from flask import render_template, request
from functions import calculate as c
from functions.utils import conecta_db

def render_contas(session):
    connection = conecta_db(session)
    page = request.args.get('page', default=1, type=int)
    limit = 50
    offset = (page - 1) * limit
    busca = request.args.get('busca_conta')

    if request.args.get('export_csv'):
        pass

    if busca:
        contas = c.listar_contas_filtrada(connection, conta=busca, offset=offset, limit=limit)
    else:
        contas = c.listar_todas_contas(connection, offset=offset, limit=limit)

    indicadores = c.indicadores_contas(connection)
    graficos = {
        'tipos': c.distribuicao_tipos(connection),
        'ativas': c.contas_ativas_inativas(connection)
    }

    for conta in contas:
        conta['tx_mes'] = c.tx_por_mes(connection, conta['idConta'])
        
    return render_template(
        'contas.html',
        contas=contas,
        page=page,
        indicadores=indicadores,
        graficos=graficos
    )
  
def render_transacoes(session: boto3.Session) -> render_template:
  connection = conecta_db(session)
  page = request.args.get('page', default=1, type=int)
  limit = 50
  offset = (page - 1) * limit
  
  conta_origem = request.args.get('busca_transacoes')
  
  if conta_origem:
      transacoes = c.listar_transacoes_filtradas(connection
                                              ,conta_origem=conta_origem
                                              ,offset=offset)
  
  else:
      transacoes = c.listar_todas_transacoes(connection
                                          ,offset=offset)
  return render_template('transacoes.html'
                          ,transacoes=transacoes
                          ,page=page)
  
def render_dashboard_investimentos(session: boto3.Session) -> render_template:
  connection = conecta_db(session)
  geral_investimentos = c.levantamento_geral_de_investimentos(connection)
  total_investido = geral_investimentos['total_investido']
  total_contas = geral_investimentos['total_contas']
  total_pessoas = geral_investimentos['total_pessoas']
  corrente, investimento = c.contar_tipos_contas(connection)
  ordens = c.ultimas_ordens(connection)
  tipo_renda_grafico = request.args.get('tipoInvestimento', 'AMBOS')
  datas, rendimentos = c.rendimento_por_mes(connection, tipo_renda_grafico)
  
  return render_template('dashboard.html', total_investido=total_investido
                         ,total_contas=total_contas, total_pessoas=total_pessoas
                         ,corrente=corrente, investimento=investimento
                         ,datas=datas, rendimentos=rendimentos
                         ,ultimas_ordens=ordens)
  