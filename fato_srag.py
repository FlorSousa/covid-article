import os
import time
import json
import datetime
import pandas as pd
from dotenv import load_dotenv
from models.srag_models import*
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func, create_engine
from urllib.parse import quote_plus as urlquote


load_dotenv()
inicio = time.time()
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('USER')}:{urlquote(os.getenv('PASSWORD'))}@{os.getenv('HOST')}:5432/{os.getenv('DB_NAME')}", echo=False)


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


def carrega_dimensao():
    try:
        print("Carregando dados para dimensao data")
        dataQuery = pd.to_datetime(pd.read_sql_query(
            'SELECT DISTINCT dt_notific FROM srag;', engine)['dt_notific'], format="%d/%m/%Y")
        dataframe_query = pd.DataFrame(dataQuery, index=None)
        lista_meses = [
            int(mes) for mes in dataframe_query['dt_notific'].dt.month.sort_values().unique()]
        lista_anos = [int(ano) for ano in dataframe_query['dt_notific'].dt.year.sort_values(
        ).unique() if int(ano) >= 2020 or int(ano) <= datetime.datetime.now().date().year]
        for mes in lista_meses:
            for ano in lista_anos:
                session.add(DimensaoData(mes=mes, ano=ano))
        print("Dados foram carregados para a dimensao data")

        print("Carregando dados para dimensao sexo")
        dataQuery = pd.read_sql_query(
            'SELECT DISTINCT "cs_sexo" FROM srag;', engine)
        dataframe_query = pd.DataFrame(dataQuery)
        valor_nulo = pd.Series(['0'], index=dataframe_query.columns)
        dataframe_query.loc[len(dataframe_query)] = valor_nulo
        sexo_relation = {"M": 1, "F": 2, "I": 3, "0": 0}

        for sexo in dataframe_query['cs_sexo']:
            if sexo in sexo_relation:
                session.add(DimensaoSexoSrag(
                    sexo=sexo, sexo_num=sexo_relation[sexo]))
        print("Dados foram carregados para a dimensao sexo")
        
        print("Carregando dados para dimensao estado")
        with open('utils\json_estados.json', encoding='utf-8') as arquivo_json:
            json_estados = json.load(arquivo_json)
            dataQuery = pd.read_sql_query('SELECT DISTINCT "sg_uf" FROM srag;', engine)
            dataframe_query = pd.DataFrame(dataQuery)
            valor_nulo = pd.Series(['0'], index=dataframe_query.columns)
            dataframe_query.loc[len(dataframe_query)] = valor_nulo

            for uf in dataframe_query["sg_uf"]:
                if uf in json_estados:
                    session.add(DimensaoEstado(
                        uf = uf,
                        nome_estado = json_estados[uf]["nome"],
                        codigo_ibge= json_estados[uf]["codigo_ibge"],
                        regiao = json_estados[uf]["regiao"]
                    ))
        print("Dados foram carregados para a dimensao estado")

        print("Carregando dados para dimensao ocupacao")

        dataQuery = pd.read_sql_query('SELECT DISTINCT pac_cocbo , lower(pac_dscbo) FROM srag;', engine)
        dataframe_cbo  = pd.DataFrame(dataQuery)
        dataframe_cbo.rename(columns={'pac_cocbo': 'CODIGO', 'lower':'TITULO'}, inplace=True)
        
        dataframe_profi_saude = pd.read_csv("utils\CBO_PROFISSIONAIS_SAUDE_1.csv", encoding="UTF-8", dtype={'CODIGO': object}, delimiter=";", index_col=None)
        dataframe_profi_geral = pd.read_csv("utils\CBO2002_Ocupacao.csv", encoding="latin_1", dtype={'CODIGO': object}, delimiter=";", index_col=None)

        dataframe_cbo = dataframe_profi_geral.merge(dataframe_profi_saude, on=['CODIGO','TITULO'], how='outer')
        dataframe_cbo['CODIGO'].fillna('0', inplace=True)
        dataframe_cbo['TITULO'].fillna('não informado', inplace=True)
        dataframe_cbo['PROFI_SAUDE'].fillna(False, inplace=True)


        dataframe_cbo = dataframe_cbo.drop_duplicates()
        
        for row in dataframe_cbo.values:
            session.add(DimensaoOcupacaoSrag(
                cbo_codigo = row[0],
                nome_profissao = row[1] ,
                profissional_saude = row[2]
            ))
            
        print("Dados foram carregados para a ocupacao")

        print("Salvando dados no Banco de dados")
        session.commit()

    except Exception as e:
        print(f"Erro: {e}")
    finally:
        print("Dimensões salvas")



if cria_dimensao():
    try:
        session = sessionmaker(bind=engine)()
        carrega_dimensao()
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
            func.coalesce(func.substr(SRAG.dt_entuti, 7, 4),
                          func.substr(SRAG.dt_notific, 7, 4)).label('ano'),
            func.coalesce(func.substr(SRAG.dt_entuti, 4, 2),
                          func.substr(SRAG.dt_notific, 4, 2)).label('mes'),
            SRAG.cs_sexo.label('sexo'),
            SRAG.sg_uf.label('uf'),
            SRAG.pac_cocbo.label('ocupacao'),
            func.count().label('internacoes')
        ).filter(SRAG.classi_fin == 5).filter(SRAG.uti == 1).group_by('ano', 'mes', 'sexo', 'uf', 'ocupacao').all())

        dataframe_entrada_uti = pd.DataFrame(session.query(
            func.coalesce(func.substr(SRAG.dt_entuti, 7, 4),
                          func.substr(SRAG.dt_notific, 7, 4)).label('ano'),
            func.coalesce(func.substr(SRAG.dt_entuti, 4, 2),
                          func.substr(SRAG.dt_notific, 4, 2)).label('mes'),
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
            func.coalesce(func.substr(SRAG.dt_saiduti, 7, 4),
                          func.substr(SRAG.dt_notific, 7, 4)).label('ano'),
            func.coalesce(func.substr(SRAG.dt_saiduti, 4, 2),
                          func.substr(SRAG.dt_notific, 4, 2)).label('mes'),
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
            func.coalesce(func.substr(SRAG.dt_evoluca, 7, 4),
                          func.substr(SRAG.dt_notific, 7, 4)).label('ano'),
            func.coalesce(func.substr(SRAG.dt_evoluca, 4, 2),
                          func.substr(SRAG.dt_notific, 4, 2)).label('mes'),
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

        tabela_fato_dataframe = pd.concat(
            [dataframe_notificacoes, dataframe_internacoes, dataframe_entrada_uti, dataframe_saida_uti, dataframe_obitos])

        tabela_fato_dataframe["sexo"] = tabela_fato_dataframe["sexo"].replace(
            'M', '1').replace('F', '2').replace('I', '3').fillna('0').astype(int)
        tabela_fato_dataframe["ano"] = tabela_fato_dataframe["ano"].fillna(
            '0').astype(int)
        tabela_fato_dataframe["mes"] = tabela_fato_dataframe["mes"].fillna(
            '0').astype(int)
        tabela_fato_dataframe["uf"] = tabela_fato_dataframe["uf"].fillna('0')
        tabela_fato_dataframe['internacoes'] = tabela_fato_dataframe['internacoes'].fillna(
            0).astype(int)
        tabela_fato_dataframe['internacoes_entrada_uti'] = tabela_fato_dataframe['internacoes_entrada_uti'].fillna(
            '0').astype(int)
        tabela_fato_dataframe['internacoes_saida_uti'] = tabela_fato_dataframe['internacoes_saida_uti'].fillna(
            0).astype(int)
        tabela_fato_dataframe['obitos'] = tabela_fato_dataframe['obitos'].fillna(
            0).astype(int)
        tabela_fato_dataframe['ocupacao'] = tabela_fato_dataframe['ocupacao'].fillna(
            '0')
        tabela_fato_dataframe['notificacoes'] = tabela_fato_dataframe['notificacoes'].fillna(
            0).astype(int)

        tabela_fato_dataframe.drop((tabela_fato_dataframe.loc[
            (tabela_fato_dataframe['ano'] > datetime.datetime.now().date().year) |
            (tabela_fato_dataframe['ano'] < 2020) | (tabela_fato_dataframe['mes'] > datetime.datetime.now().date().month)]).index, inplace=True)

        for index, row in tabela_fato_dataframe.iterrows():
            session.add(FatoSrag(
                id_cbo_fato=session.query(DimensaoOcupacaoSrag).filter_by(
                    cbo_codigo=row['ocupacao']).first(),
                id_ano_fato=session.query(DimensaoData).filter_by(
                    ano=str(row['ano'])).first(),
                id_mes_fato=session.query(DimensaoData).filter_by(
                    mes=row['mes']).first(),
                id_sexo_fato=session.query(DimensaoSexoSrag).filter_by(
                    sexo_num=row['sexo']).first(),
                id_estado_fato=session.query(
                    DimensaoEstado).filter_by(uf=row['uf']).first(),
                notificacoes=row['notificacoes'],
                internacoes=row['internacoes'],
                internacoes_entrada_uti=row['internacoes_entrada_uti'],
                internacoes_saida_uti=row['internacoes_saida_uti'],
                obitos=row['obitos']
            ))

            session.commit()

        session.close()
    except Exception as e:
        print(f"Erro:{e}")

print("fim")
fim = time.time()
print(f"Tempo total: {(fim - inicio) / 60}")
