CREATE TABLE IF NOT EXISTS TRANSACOES (
    idTransacao INT PRIMARY KEY,
    contaOrigem INT NOT NULL,
    contaDestino INT NOT NULL,
    idCartao INT NOT NULL,
    tipoTransacao VARCHAR(7) NOT NULL,
    valorTransacao FLOAT NOT NULL,
    dataTransacao TIMESTAMP NOT NULL,
    FOREIGN KEY (contaOrigem) REFERENCES CONTACORRENTE(idConta),
    FOREIGN KEY (contaDestino) REFERENCES CONTACORRENTE(idConta),
    FOREIGN KEY (idCartao) REFERENCES CARTAOCREDITO(idCartao)
)