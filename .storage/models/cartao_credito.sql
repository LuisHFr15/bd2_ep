CREATE TABLE IF NOT EXISTS CARTAOCREDITO (
    idCartao INT PRIMARY KEY,
    idConta INT NOT NULL,
    nroCartao VARCHAR(16) NOT NULL,
    cvc INT NOT NULL,
    validade DATE NOT NULL,
    nomeCartao VARCHAR(30) NOT NULL,
    limiteCartao INT NOT NULL,
    faturaCartao FLOAT DEFAULT 0.00,
    ativo CHAR(1),
    FOREIGN KEY (idConta) REFERENCES CONTA(idConta)
)