from pathlib import Path
import os

def get_root_folder() -> str:
    current_file = Path(__file__).resolve()
    root = Path(current_file.parent).resolve()
    while('__main__.py' not in os.listdir(root)):
        root = Path(root.parent).resolve()
        
    return root

def get_random_seed() -> int:
    import time
    
    seed: int = int(time.time())
    return seed 

def identifica_tipo_codigo(codigo: str) -> str:
    tamanho_codigo = len(codigo)
    if tamanho_codigo == 11:
        return 'F'
    elif tamanho_codigo == 13:
        return 'J'
    else:
        return 'Invalid'
    