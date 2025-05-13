CREATE TABLE IF NOT EXISTS PESSOA (
    codigoPessoa VARCHAR(14) PRIMARY KEY,
    tipoCodigo CHAR(1) NOT NULL,
    quantidadeContas INT NOT NULL,
    emailComunicacao VARCHAR(30) NOT NULL,
    logCriacao TIMESTAMP NOT NULL
)
