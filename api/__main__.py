import os
from flask import Flask, render_template, request
from functions.utils import conecta_sessao_aws
from functions import calculate as c

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
static_dir = os.path.join(BASE_DIR, 'src', 'static')
template_dir = os.path.join(BASE_DIR, 'src', 'templates')

dbsysbank = Flask("dbsysbank"
                ,template_folder=template_dir
                ,static_folder=static_dir)

session = conecta_sessao_aws()

@dbsysbank.route('/')
def main() -> render_template:
    return render_template('base.html', title='DBSysBank - Menu')

@dbsysbank.route('/contas')
def contas() -> render_template:
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

@dbsysbank.route('/dashboard')
def dashboard() -> render_template:
    return render_template('dashboard.html')

@dbsysbank.route('/transacoes')
def transacoes() -> render_template:
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
    
if __name__ == '__main__':
    dbsysbank.run(debug=True)