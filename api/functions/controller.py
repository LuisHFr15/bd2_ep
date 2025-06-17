import boto3
from flask import render_template, request
from functions import calculate as c

def render_contas(session: boto3.Session) -> render_template:
  page = request.args.get('page', default=1, type=int)
  limit = 50
  offset = (page - 1) * limit
  
  conta_buscada = request.args.get('busca_conta')
  if conta_buscada:
      contas = c.listar_contas_filtrada(session
                                      ,conta=conta_buscada
                                      ,offset=offset)
  else:
      contas = c.listar_todas_contas(session
                                  ,offset=offset)
      
  return render_template('contas.html', contas=contas, page=page)

def render_transacoes(session: boto3.Session) -> render_template:
  page = request.args.get('page', default=1, type=int)
  limit = 50
  offset = (page - 1) * limit
  
  conta_origem = request.args.get('busca_transacoes')
  
  if conta_origem:
      transacoes = c.listar_transacoes_filtradas(session
                                              ,conta_origem=conta_origem
                                              ,offset=offset)
  
  else:
      transacoes = c.listar_todas_transacoes(session
                                          ,offset=offset)
  return render_template('transacoes.html'
                          ,transacoes=transacoes
                          ,page=page)