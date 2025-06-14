from flask import Flask, render_template, request
from functions.utils import conecta_db, conecta_sessao_aws
from functions import calculate as c
dbsysbank = Flask("dbsysbank", template_folder='./src/templates/')

session = conecta_sessao_aws()
con = conecta_db(session)

@dbsysbank.route('/')
def main() -> render_template:
    return render_template('base.html', title='DBSysBank - Menu')

@dbsysbank.route('/contas')
def contas() -> render_template:
    return render_template('contas.html')

@dbsysbank.route('/dashboard')
def dashboard() -> render_template:
    return render_template('dashboard.html')

@dbsysbank.route('/transacoes')
def transacoes() -> render_template:
    conta_origem = request.args.get('busca_transacoes')
    page = request.args.get('page', default=1, type=int)
    limit = 50
    offset = (page - 1) * limit
    
    if conta_origem:
        transacoes = c.listar_todas_transacoes(con,
                                               conta_origem=conta_origem
                                               ,offset=offset)
    
    else:
        transacoes = c.listar_todas_transacoes(con
                                               ,offset=offset)
    return render_template('transacoes.html'
                           ,transacoes=transacoes
                           ,page=page)
    
if __name__ == '__main__':
    if con:
        print('Conectado ao banco')
    dbsysbank.run(debug=True)