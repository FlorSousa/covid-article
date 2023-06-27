import os
import time
import pandas as pd
from dotenv import load_dotenv
from models.srag_models import *
from sqlalchemy import inspect, create_engine
from urllib.parse import quote_plus as urlquote
load_dotenv()
inicio = time.time()
engine = create_engine(f"postgresql+psycopg2://{os.getenv('USER')}:{urlquote(os.getenv('PASSWORD'))}@{os.getenv('HOST')}:5432/{os.getenv('DB_NAME')}",echo=False)

def cria_dimensao():
    try:
        print("Iniciando conexão")
        print("Conexão feita")
        print("Removendo as dimensões existentes")
        Base.metadata.drop_all(engine)
        print("Criando as dimensões")
        Base.metadata.create_all(engine) 
        print("Dimensões criadas")
    except Exception as e:
        print(F"Erro:{e}")

cria_dimensao()

fim = time.time()
print(f"Tempo total: {(fim-inicio)/60}")
