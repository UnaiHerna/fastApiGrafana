from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class VariableSchema(BaseModel):
    id: int
    simbolo: str
    u_medida: str
    descripcion: str

    class Config:
        orm_mode = True

class EquipoSchema(BaseModel):
    id: int
    nombre: str
    descripcion: str

    class Config:
        orm_mode = True

class SensorSchema(BaseModel):
    id_equipo: int
    id_variable: int
    deltat: Optional[int]

    class Config:
        orm_mode = True

class SensorDatosSchema(BaseModel):
    id_equipo: int
    id_variable: int
    timestamp: datetime
    valor: Optional[float]

    class Config:
        orm_mode = True

class SenalSchema(BaseModel):
    id: int
    nombre: str

    class Config:
        orm_mode = True

class SenalSensorSchema(BaseModel):
    id_señal: int
    id_equipo: int
    id_variable: int

    class Config:
        orm_mode = True

class SenalDatosSchema(BaseModel):
    id_señal: int
    timestamp: datetime
    valor: Optional[float]

    class Config:
        orm_mode = True

class ConsignaSchema(BaseModel):
    id: int
    id_equipo: int
    id_variable: int
    nombre: str

    class Config:
        orm_mode = True

class ValoresConsignaSchema(BaseModel):
    id_consigna: int
    timestamp: datetime
    valor: Optional[float]
    mode: Optional[int]

    class Config:
        orm_mode = True

class HLCSchema(BaseModel):
    id: int
    id_consigna_entrada: int
    id_consigna_salida: int
    nombre: str

    class Config:
        orm_mode = True

class LLCSchema(BaseModel):
    id: int
    id_consigna: int
    nombre: str

    class Config:
        orm_mode = True

class ActuadorSchema(BaseModel):
    id: int
    id_llc: int
    nombre: str

    class Config:
        orm_mode = True

class ActuadorDatosSchema(BaseModel):
    id_actuador: int
    timestamp: datetime

    class Config:
        orm_mode = True

class UserSchema(BaseModel):
    id: int
    username: str
    email: str
    password: str
    disabled: Optional[bool] = True

    class Config:
        orm_mode = True