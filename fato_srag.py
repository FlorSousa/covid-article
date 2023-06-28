import os
import time
import pandas as pd
from dotenv import load_dotenv
from models.srag_models import*
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func, create_engine
from urllib.parse import quote_plus as urlquote

load_dotenv()
inicio = time.time()
engine = create_engine(f"postgresql+psycopg2://{os.getenv('USER')}:{urlquote(os.getenv('PASSWORD'))}@{os.getenv('HOST')}:5432/{os.getenv('DB_NAME')}", echo=False)

def cria_dimensao():
    try:
        print("Iniciando conexão")
        print("Conexão feita")
        print("Removendo as dimensões existentes")
        Base.metadata.remove(SRAG.__table__)
        Base.metadata.drop_all(engine)
        print("Criando as dimensões")
        Base.metadata.create_all(engine) 
        print("Dimensões criadas")
        return True
    except Exception as e:
        print(f"Erro: {e}")
        return False

if cria_dimensao():
    session = sessionmaker(bind=engine)()
    
    dataframe_notificacoes = pd.DataFrame(session.query(
        func.substr(SRAG.dt_notific, 7, 4).label('ano'),
        func.substr(SRAG.dt_notific, 4, 2).label('mes'),
        SRAG.cs_sexo.label('sexo'),
        SRAG.sg_uf.label('uf'),
        SRAG.pac_cocbo.label('ocupacao'),
        func.count().label('notificacoes')
    ).filter(
        SRAG.classi_fin == 5
    ).group_by(
        'ano', 'mes', 'sexo', 'uf', 'ocupacao'
    ).all())

    dataframe_internacoes = pd.DataFrame(session.query(
        func.coalesce(func.substr(SRAG.dt_entuti, 7, 4), func.substr(SRAG.dt_notific, 7, 4)).label('ano'),
        func.coalesce(func.substr(SRAG.dt_entuti, 4, 2), func.substr(SRAG.dt_notific, 4, 2)).label('mes'),
        SRAG.cs_sexo.label('sexo'),
        SRAG.sg_uf.label('uf'),
        SRAG.pac_cocbo.label('ocupacao'),
        func.count().label('internacoes_entrada_uti')
    ).filter(SRAG.classi_fin == 5).filter(SRAG.uti == 1).group_by('ano', 'mes', 'sexo', 'uf', 'ocupacao').all())

    dataframe_entrada_uti = pd.DataFrame(session.query(
        func.coalesce(func.substr(SRAG.dt_entuti, 7, 4), func.substr(SRAG.dt_notific, 7, 4)).label('ano'),
        func.coalesce(func.substr(SRAG.dt_entuti, 4, 2), func.substr(SRAG.dt_notific, 4, 2)).label('mes'),
        SRAG.cs_sexo.label('sexo'),
        SRAG.sg_uf.label('uf'),
        SRAG.pac_cocbo.label('ocupacao'),
        func.count().label('internacoes_entrada_uti')
    ).filter(
        SRAG.classi_fin == 5,
        SRAG.uti == 1
    ).group_by(
        'ano', 'mes', 'sexo', 'uf', 'ocupacao'
    ).all())

    dataframe_saida_uti = pd.DataFrame(session.query(
        func.coalesce(func.substr(SRAG.dt_saiduti, 7, 4), func.substr(SRAG.dt_notific, 7, 4)).label('ano'),
        func.coalesce(func.substr(SRAG.dt_saiduti, 4, 2), func.substr(SRAG.dt_notific, 4, 2)).label('mes'),
        SRAG.cs_sexo.label('sexo'),
        SRAG.sg_uf.label('uf'),
        SRAG.pac_cocbo.label('ocupacao'),
        func.count().label('internacoes_saida_uti')
    ).filter(
        SRAG.classi_fin == 5,
        SRAG.evolucao == 2
    ).group_by(
        'ano', 'mes', 'sexo', 'uf', 'ocupacao'
    ).all())

    dataframe_obitos = pd.DataFrame(session.query(
        func.coalesce(func.substr(SRAG.dt_evoluca, 7, 4), func.substr(SRAG.dt_notific, 7, 4)).label('ano'),
        func.coalesce(func.substr(SRAG.dt_evoluca, 4, 2), func.substr(SRAG.dt_notific, 4, 2)).label('mes'),
        SRAG.cs_sexo.label('sexo'),
        SRAG.sg_uf.label('uf'),
        SRAG.pac_cocbo.label('ocupacao'),
        func.count().label('obitos')
    ).filter(
        SRAG.classi_fin == 5,
        SRAG.evolucao == 2
    ).group_by(
        'ano', 'mes', 'sexo', 'uf', 'ocupacao'
    ).all())
    

    tabela_fato_dataframe = pd.concat([dataframe_notificacoes,dataframe_internacoes,dataframe_entrada_uti, dataframe_saida_uti,dataframe_obitos])
    

    session.close()

fim = time.time()
print(f"Tempo total: {(fim - inicio) / 60}")
