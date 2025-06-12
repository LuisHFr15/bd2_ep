from flask import Flask, render_template
from functions.utils import conecta_db, conecta_sessao_aws
dbsysbank = Flask("dbsysbank")

session = conecta_sessao_aws()
con = conecta_db(session)

@dbsysbank.route('/')
def main() -> None:
    return render_template('base.html', title='DBSysBank - Menu')

    
if __name__ == '__main__':
    dbsysbank.run(debug=True)