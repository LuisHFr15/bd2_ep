from utils.sys_func import identifica_tipo_codigo
import time
from datetime import datetime

class Pessoa():
    def __init__(self, codigo: str, email_comunicacao: str, quantidade_contas: int = None):
        tipo_codigo = identifica_tipo_codigo(codigo)
        if tipo_codigo == 'Invalid':
            raise ValueError("Código inválido para o usuário")
        
        self.__codigo_pessoa = codigo
        self._tipo_codigo = tipo_codigo
        if quantidade_contas is not None:
            self._quantidade_contas = quantidade_contas
        else:
            self._quantidade_contas = 0
        self._email_comunicacao = email_comunicacao
        self._log_criacao = datetime.fromtimestamp(time.time())
        
    def __str__(self):
        return f'A pessoa de código {self.__codigo_pessoa} é do tipo {self._tipo_codigo} e possui {self._quantidade_contas} contas'