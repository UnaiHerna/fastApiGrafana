# models.py
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, ForeignKeyConstraint, UniqueConstraint, \
    Boolean
from db.connector import Base


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
    descripcion = Column(String(100))


class Sensor(Base):
    __tablename__ = 'sensor'
    id_equipo = Column(Integer, ForeignKey('equipo.id'), primary_key=True, nullable=False)
    id_variable = Column(Integer, ForeignKey('variable.id'), primary_key=True, nullable=False)


class SensorDatos(Base):
    __tablename__ = 'sensor_datos'
    id_equipo = Column(Integer, primary_key=True, nullable=False)
    id_variable = Column(Integer, primary_key=True, nullable=False)
    timestamp = Column(DateTime, primary_key=True, nullable=False)
    valor = Column(Float)
    __table_args__ = (
        ForeignKeyConstraint(['id_equipo', 'id_variable'], ['sensor.id_equipo', 'sensor.id_variable']),
    )


class Senal(Base):
    __tablename__ = 'señal'
    id = Column(Integer, primary_key=True, nullable=False)
    nombre = Column(String(50))


class SenalSensor(Base):
    __tablename__ = 'señal_sensor'
    id_señal = Column(Integer, ForeignKey('señal.id'), primary_key=True, nullable=False)
    id_equipo = Column(Integer, primary_key=True, nullable=False)
    id_variable = Column(Integer, primary_key=True, nullable=False)
    __table_args__ = (
        ForeignKeyConstraint(['id_equipo', 'id_variable'], ['sensor.id_equipo', 'sensor.id_variable']),
    )


class SenalDatos(Base):
    __tablename__ = 'señal_datos'
    id_señal = Column(Integer, ForeignKey('señal.id'), primary_key=True, nullable=False)
    timestamp = Column(DateTime, primary_key=True, nullable=False)
    valor = Column(Float)


class Consigna(Base):
    __tablename__ = 'consigna'
    id = Column(Integer, primary_key=True, nullable=False)
    id_equipo = Column(Integer, nullable=False)
    id_variable = Column(Integer, nullable=False)
    nombre = Column(String(50))

    __table_args__ = (
        UniqueConstraint('id_equipo', 'id_variable'),
        ForeignKeyConstraint(['id_equipo', 'id_variable'], ['sensor.id_equipo', 'sensor.id_variable']),
    )


class ValoresConsigna(Base):
    __tablename__ = 'valores_consigna'
    id_consigna = Column(Integer, ForeignKey('consigna.id'), primary_key=True, nullable=False)
    timestamp = Column(DateTime, primary_key=True, nullable=False)
    valor = Column(Float)
    mode = Column(Integer)


class HLC(Base):
    __tablename__ = 'hlc'
    id = Column(Integer, primary_key=True, nullable=False)
    id_consigna_entrada = Column(Integer, ForeignKey('consigna.id'), primary_key=True, nullable=False)
    id_consigna_salida = Column(Integer, ForeignKey('consigna.id'), primary_key=True, nullable=False)
    nombre = Column(String(50))


class LLC(Base):
    __tablename__ = 'llc'
    id = Column(Integer, primary_key=True, nullable=False)
    id_consigna = Column(Integer, ForeignKey('consigna.id'), primary_key=True, nullable=False)
    nombre = Column(String(50))


class Actuador(Base):
    __tablename__ = 'actuador'
    id = Column(Integer, primary_key=True, nullable=False)
    id_llc = Column(Integer, ForeignKey('llc.id'), nullable=False)
    nombre = Column(String(50))


class ActuadorDatos(Base):
    __tablename__ = 'actuador_datos'
    id_actuador = Column(Integer, ForeignKey('actuador.id'), primary_key=True, nullable=False)
    timestamp = Column(DateTime, primary_key=True, nullable=False)
