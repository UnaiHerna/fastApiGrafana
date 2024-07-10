# models.py
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from connector import Base


class Variable(Base):
    __tablename__ = 'variable'
    id = Column(Integer, primary_key=True, nullable=False)
    simbolo = Column(String(10))
    u_medida = Column(String(10))
    descripcion = Column(String(50))


class Equipo(Base):
    __tablename__ = 'equipo'
    id = Column(Integer, primary_key=True, nullable=False)
    nombre = Column(String(50))
    descripcion = Column(String(50))


class Relacion(Base):
    __tablename__ = 'relacion'
    id_equipo = Column(Integer, ForeignKey('equipo.id'), primary_key=True, nullable=False)
    id_variable = Column(Integer, ForeignKey('variable.id'), primary_key=True, nullable=False)


class Datos(Base):
    __tablename__ = 'datos'
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    id_equipo = Column(Integer, ForeignKey('relacion.id_equipo'), nullable=False)
    id_variable = Column(Integer, ForeignKey('relacion.id_variable'), nullable=False)
    timestamp = Column(DateTime)
    valor = Column(Float)
