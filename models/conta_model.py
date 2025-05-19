from datetime import datetime

class Conta:
    def __init__(
        self,
        codigo_pessoa: str,
        agencia: int,
        nro_conta: int,
        senha_conta: str,
        renda: float,
        perfil_credito: str,
        ativa: str,
        data_criacao: datetime = None,
        id_conta: int = None
    ):
        self.id_conta = id_conta
        self.codigo_pessoa = codigo_pessoa
        self.agencia = agencia
        self.nro_conta = nro_conta
        self.senha_conta = senha_conta
        self.renda = renda
        self.perfil_credito = perfil_credito
        self.ativa = ativa
        self.data_criacao = data_criacao or datetime.now()

    def __str__(self):
        return (
            f'Conta {self.nro_conta} (Agência {self.agencia}) '
            f'de {self.codigo_pessoa} | Perfil: {self.perfil_credito} | '
            f'Ativa: {"Sim" if self.ativa == "S" else "Não"}'
        )