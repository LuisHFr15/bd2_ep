from flask import Flask, render_template, request
import sqlite3

app = Flask(__name__)

def get_pessoas(filtro_codigo="", filtro_email=""):
    con = sqlite3.connect('.storage/database.db')
    cursor = con.cursor()
    query = "SELECT codigoPessoa, tipoCodigo, quantidadeContas, emailComunicacao, logCriacao FROM PESSOA WHERE 1=1"
    params = []
    if filtro_codigo.strip():
        query += " AND codigoPessoa LIKE ?"
        params.append(f"%{filtro_codigo}%")
    if filtro_email.strip():
        query += " AND emailComunicacao LIKE ?"
        params.append(f"%{filtro_email}%")
    cursor.execute(query, params)
    pessoas = cursor.fetchall()
    con.close()
    return pessoas

def get_contas(filtro_codigo="", filtro_agencia="", filtro_perfil=""):
    con = sqlite3.connect('.storage/database.db')
    cursor = con.cursor()
    query = "SELECT codigoPessoa, agencia, nroConta, renda, perfilCredito, ativa, dataCriacao FROM CONTA WHERE 1=1"
    params = []
    if filtro_codigo.strip():
        query += " AND codigoPessoa LIKE ?"
        params.append(f"%{filtro_codigo}%")
    if filtro_agencia.strip():
        query += " AND agencia = ?"
        params.append(filtro_agencia)
    if filtro_perfil.strip():
        query += " AND perfilCredito LIKE ?"
        params.append(f"%{filtro_perfil}%")
    cursor.execute(query, params)
    contas = cursor.fetchall()
    con.close()
    return contas

@app.route('/', methods=['GET', 'POST'])
def index():
    aba = request.form.get('aba') if request.method == 'POST' else 'pessoa'
    # Pessoa
    filtro_codigo_pessoa = request.form.get('codigo_pessoa', "")
    filtro_email = request.form.get('email', "")
    pessoas = get_pessoas(filtro_codigo_pessoa, filtro_email) if aba == 'pessoa' else []
    # Conta
    filtro_codigo_conta = request.form.get('codigo_conta', "")
    filtro_agencia = request.form.get('agencia', "")
    filtro_perfil = request.form.get('perfil', "")
    contas = get_contas(filtro_codigo_conta, filtro_agencia, filtro_perfil) if aba == 'conta' else []
    return render_template(
        'index.html',
        aba=aba,
        pessoas=pessoas,
        contas=contas,
        filtro_codigo_pessoa=filtro_codigo_pessoa,
        filtro_email=filtro_email,
        filtro_codigo_conta=filtro_codigo_conta,
        filtro_agencia=filtro_agencia,
        filtro_perfil=filtro_perfil
    )

if __name__ == '__main__':
    app.run(debug=True)