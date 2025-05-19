from flask import Flask, render_template, request
import sqlite3
import math

app = Flask(__name__)

REGISTROS_POR_PAGINA = 100

def get_pessoas(filtro_codigo="", filtro_email="", pagina=1):
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
    # Conta total para paginação
    count_query = "SELECT COUNT(*) FROM (" + query + ")"
    cursor.execute(count_query, params)
    total = cursor.fetchone()[0]
    offset = (pagina - 1) * REGISTROS_POR_PAGINA
    query += " LIMIT ? OFFSET ?"
    params.extend([REGISTROS_POR_PAGINA, offset])
    cursor.execute(query, params)
    pessoas = cursor.fetchall()
    con.close()
    return pessoas, total

def get_contas(filtro_codigo="", filtro_agencia="", filtro_perfil="", pagina=1):
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
    # Conta total para paginação
    count_query = "SELECT COUNT(*) FROM (" + query + ")"
    cursor.execute(count_query, params)
    total = cursor.fetchone()[0]
    offset = (pagina - 1) * REGISTROS_POR_PAGINA
    query += " LIMIT ? OFFSET ?"
    params.extend([REGISTROS_POR_PAGINA, offset])
    cursor.execute(query, params)
    contas = cursor.fetchall()
    con.close()
    return contas, total

@app.route('/', methods=['GET', 'POST'])
def index():
    aba = request.form.get('aba') if request.method == 'POST' else request.args.get('aba', 'pessoa')
    pagina = int(request.args.get('pagina', 1))

    
    # Pessoa
    filtro_codigo_pessoa = request.form.get('codigo_pessoa', "") if request.method == 'POST' else request.args.get('codigo_pessoa', "")
    filtro_email = request.form.get('email', "") if request.method == 'POST' else request.args.get('email', "")
    pessoas, total_pessoas = get_pessoas(filtro_codigo_pessoa, filtro_email, pagina) if aba == 'pessoa' else ([], 0)
    # Conta
    filtro_codigo_conta = request.form.get('codigo_conta', "") if request.method == 'POST' else request.args.get('codigo_conta', "")
    filtro_agencia = request.form.get('agencia', "") if request.method == 'POST' else request.args.get('agencia', "")
    filtro_perfil = request.form.get('perfil', "") if request.method == 'POST' else request.args.get('perfil', "")
    contas, total_contas = get_contas(filtro_codigo_conta, filtro_agencia, filtro_perfil, pagina) if aba == 'conta' else ([], 0)
    total_registros = total_pessoas if aba == 'pessoa' else total_contas
    total_paginas = math.ceil(total_registros / REGISTROS_POR_PAGINA) if total_registros else 1
    return render_template(
        'index.html',
        aba=aba,
        pessoas=pessoas,
        contas=contas,
        filtro_codigo_pessoa=filtro_codigo_pessoa,
        filtro_email=filtro_email,
        filtro_codigo_conta=filtro_codigo_conta,
        filtro_agencia=filtro_agencia,
        filtro_perfil=filtro_perfil,
        pagina=pagina,
        total_paginas=total_paginas,
        max =max,
        min=min
    )

if __name__ == '__main__':
    app.run(debug=True)