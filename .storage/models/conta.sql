CREATE TABLE IF NOT EXISTS CONTA (
    idConta INTEGER PRIMARY KEY AUTOINCREMENT,
    codigoPessoa VARCHAR(14) NOT NULL,
    agencia INT NOT NULL,
    nroConta INT NOT NULL,
    senhaConta VARCHAR(30) NOT NULL,
    renda FLOAT,
    perfilCredito VARCHAR(20),
    ativa CHAR(1),
    dataCriacao TIMESTAMP NOT NULL,
    FOREIGN KEY (codigoPessoa) REFERENCES PESSOA(codigoPessoa)
)