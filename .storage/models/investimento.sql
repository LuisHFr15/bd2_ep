CREATE TABLE IF NOT EXISTS INVESTIMENTO (
    idInvest INT PRIMARY KEY,
    cnpjOferecedor VARCHAR(14),
    diasCotizacao INT NOT NULL,
    diasRetirada INT NOT NULL,
    totalInvestido FLOAT DEFAULT 0.00,
    tipoRenda VARCHAR(10) NOT NULL,
    rendMedioMes FLOAT NOT NULL,
    totalContas INT DEFAULT 0,
    ativo CHAR(1),
    FOREIGN KEY (cnpjOferecedor) REFERENCES PESSOAJURIDICA(cnpj)
)