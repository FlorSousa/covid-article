import os
import time
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import  create_engine
from urllib.parse import quote_plus as urlquote

load_dotenv()

def make_dataframe():
    return pd.concat([pd.DataFrame(pd.read_csv("data/"+filename,sep=";",low_memory=False)) for filename in os.listdir("data")])

inicio = time.time()
try:
    print("Iniciando conexão")
    url = "postgresql://%s:%s@%s" % (os.getenv('USER'), urlquote(os.getenv('PASSWORD')),
                                    os.getenv('HOST') + ":5432/" + os.getenv('DB_NAME'))
    engine = create_engine(url, echo = True ).connect()
    print("Conexão feita")

    print("Montando tabela povoamento_srag")
    dataframe = make_dataframe()
    dataframe.columns = [col.lower() for col in dataframe.columns]
    dataframe.to_sql('srag', con = engine, index=False,if_exists='replace',schema='public',chunksize=100000)
    print("Fim do processo de povoamento")
except Exception as e:
    print(f'Erro:{e}')

fim = time.time()
print(f"Tempo total: {(fim-inicio)/60}")