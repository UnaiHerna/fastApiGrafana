from typing import Optional

from fastapi import Depends, HTTPException, APIRouter, status
from sqlalchemy.orm import Session
from sqlalchemy import select
from db.connector import get_db
from db.models import *
from db.redis_client import set_cached_response, get_cached_response


router = APIRouter(
    prefix="/datos/sensor",
    tags=["sensor"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


def read_datos_sensor_by_variable(db, variable):
    cache_key = f"datos_sensor_{variable}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    query = (
        select(
            SensorDatos.timestamp.label('time'),
            SensorDatos.valor.label('value'),
            Equipo.descripcion.label('equipo')
        )
        .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
        .join(Variable, Sensor.id_variable == Variable.id)
        .join(Equipo, Sensor.id_equipo == Equipo.id)
        .where(Variable.simbolo == variable)
        .order_by(SensorDatos.timestamp.asc())
    )
    resultados = db.execute(query).fetchall()
    datos = [{"time": r.time, "value": r.value, "equipo": r.equipo} for r in resultados]

    set_cached_response(cache_key, datos)
    return datos


def read_datos_sensor_by_equipo(db, equipo):
    cache_key = f"datos_sensor_{equipo}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    query = (
        select(
            SensorDatos.timestamp.label('time'),
            SensorDatos.valor.label('value'),
            Variable.simbolo.label('variable'),
            Equipo.descripcion.label('equipo')
        )
        .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
        .join(Variable, Sensor.id_variable == Variable.id)
        .join(Equipo, Sensor.id_equipo == Equipo.id)
        .where(Equipo.nombre == equipo)
        .order_by(SensorDatos.timestamp.asc())
    )
    resultados = db.execute(query).fetchall()
    datos = [{"time": r.time, "value": r.value, "variable": r.variable, "equipo": r.equipo} for r in resultados]

    set_cached_response(cache_key, datos)
    return datos


def read_datos_sensor_multiple_by_equipos(db, equipos):
    equipo_list = equipos.split(',')
    all_data = {}
    for equipo in equipo_list:
        data = read_datos_sensor_by_variable(db, equipo)
        all_data[equipo] = data
    return all_data


def read_datos_sensor_multiple_by_variable(db, variables):
    variable_list = variables.split(',')
    all_data = {}
    for variable in variable_list:
        data = read_datos_sensor_by_variable(db, variable)
        all_data[variable] = data
    return all_data


@router.get("/")
def datos_condicionales_sensor(
        variable: Optional[str] = None,
        variables: Optional[str] = None,
        equipo: Optional[str] = None,
        equipos: Optional[str] = None,
        db: Session = Depends(get_db)
):
    if variable and not equipo and not variables and not equipos:
        # Lógica para manejar la solicitud cuando solo se proporciona el símbolo
        return read_datos_sensor_by_variable(db, variable)
    elif equipo and not variable and not variables and not equipos:
        # Lógica para manejar la solicitud cuando solo se proporciona el nombre
        return read_datos_sensor_by_equipo(db, equipo)
    elif variables and not equipo and not variable and not equipos:
        return read_datos_sensor_multiple_by_variable(db, variables)
    elif equipos and not equipo and not variable and not variables:
        return read_datos_sensor_multiple_by_equipos(db, equipos)
    else:
        # Lógica para manejar la solicitud cuando no se proporciona ninguno de los parámetros esperados
        raise HTTPException(status_code=400, detail="Debe proporcionar los datos de forma correcta.")
