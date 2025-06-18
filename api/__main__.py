import os
from flask import Flask, render_template
from functions.utils import conecta_sessao_aws, formatar_compacto
from functions import controller as ctrl

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
static_dir = os.path.join(BASE_DIR, 'src', 'static')
template_dir = os.path.join(BASE_DIR, 'src', 'templates')

dbsysbank = Flask("dbsysbank"
                ,template_folder=template_dir
                ,static_folder=static_dir)
dbsysbank.jinja_env.filters['compacto'] = formatar_compacto


session = conecta_sessao_aws()

@dbsysbank.route('/')
def main() -> render_template:
    return render_template('base.html', title='DBSysBank - Menu')

@dbsysbank.route('/contas')
def contas() -> render_template:
    return ctrl.render_contas(session)

@dbsysbank.route('/transacoes')
def transacoes() -> render_template:
    return ctrl.render_transacoes(session)

@dbsysbank.route('/dashboard')
def dashboard() -> render_template:
    return ctrl.render_dashboard_investimentos(session)


if __name__ == '__main__':
    dbsysbank.run(debug=True)