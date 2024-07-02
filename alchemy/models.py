# models.py
from alchemy.connector import Base
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
class Elemento(Base):
    __tablename__ = 'elemento'
    id = Column(Integer, primary_key=True, nullable=False)
    simbolo = Column(String(10))
    u_medida = Column(String(10))
    descripcion = Column(String(50))

class Receptor(Base):
    __tablename__ = 'receptor'
    id = Column(Integer, primary_key=True, nullable=False)
    timestamp = Column(DateTime)
    valor = Column(Float)
    nombre = Column(String(50))

class Relacion(Base):
    __tablename__ = 'relacion'
    id_receptor = Column(Integer, ForeignKey('receptor.id'), primary_key=True, nullable=False)
    id_elemento = Column(Integer, ForeignKey('elemento.id'), primary_key=True, nullable=False)