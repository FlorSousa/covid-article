from sqlalchemy import Column, ForeignKey, Integer, String, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class SRAG(Base):
        __tablename__ = 'srag'

        id = Column(Integer, primary_key=True)
        dt_evoluca = Column(String)
        dt_interna = Column
        dt_notific = Column(String)
        cs_sexo = Column(String)
        sg_uf = Column(String)
        pac_cocbo = Column(String)
        classi_fin = Column(Integer)
        evolucao = Column(Integer)
        hospital = Column(Float)
        dt_entuti = Column(String)
        dt_saiduti = Column(String)
        uti = Column(Float)

class DimensaoData(Base):
    __tablename__ = 'dimensao_data_srag'
    
    id = Column(Integer, primary_key=True)
    data = Column(String(8), nullable=False, default="1/2022")
    mes = Column(Integer, default=1)
    ano = Column(String, default=2022)

class DimensaoSexoSrag(Base):
    __tablename__ = 'dimensao_sexo_srag'

    id = Column(Integer, primary_key=True)
    sexo = Column(String(2), nullable=False, default='1')
    sexo_num = Column(Integer, nullable=False, default=1)

class DimensaoEstado(Base):
    __tablename__ = 'dimensao_estado_srag'
    id = Column(Integer, primary_key=True)
    nome_estado = Column(String(40), nullable=False, default='1')
    uf = Column(String(2), nullable=False, default='PB')
    regiao = Column(String(40), nullable=False, default='regiao')
    codigo_ibge = Column(Integer, nullable=False, default=0)

class DimensaoOcupacaoSrag(Base):
    __tablename__ = 'dimensao_ocupacao_srag'
    
    id = Column(Integer, primary_key=True)
    cbo_codigo = Column(String(255), nullable=False, default='1')
    nome_profissao = Column(String(255), nullable=False, default='profissao')
    profissional_saude = Column(Boolean, nullable=False, default=True)

class FatoSrag(Base):
    __tablename__ = 'fato_srag'
    
    id = Column(Integer, primary_key=True)
    id_cbo_fato = Column(Integer, ForeignKey(DimensaoOcupacaoSrag.id), nullable=False)
    id_ano_fato = Column(Integer, ForeignKey(DimensaoData.id), nullable=True)
    id_mes_fato = Column(Integer, ForeignKey(DimensaoData.id), nullable=True)
    id_sexo_fato = Column(Integer, ForeignKey(DimensaoSexoSrag.id), nullable=False)
    id_estado_fato = Column(Integer, ForeignKey(DimensaoEstado.id), nullable=False)
    notificacoes = Column(Integer,nullable=False,default = 0)
    internacoes = Column(Integer,nullable=False,default = 0)
    internacoes_entrada_uti = Column(Integer,nullable=False,default = 0)
    internacoes_saida_uti = Column(Integer,nullable=False,default = 0)
    obitos = Column(Integer,nullable=False,default = 0)

    cbo_fato = relationship(DimensaoOcupacaoSrag, backref="fatos")
    ano_fato = relationship(DimensaoData, foreign_keys=[id_ano_fato], backref="fatos_ano")
    mes_fato = relationship(DimensaoData, foreign_keys=[id_mes_fato], backref="fatos_mes")
    sexo_fato = relationship(DimensaoSexoSrag, backref="fatos")
    estado_fato = relationship(DimensaoEstado, backref="fatos")
