from typing import Optional
from fastapi import Depends, HTTPException, APIRouter, status
from sqlalchemy.orm import Session
from sqlalchemy import select
from db.connector import get_db
from db.models import *
from db.redis_client import set_cached_response, get_cached_response
from datetime import datetime

from utils.date_checker import date_checker

router = APIRouter(
    prefix="/datos/sensorvacio",
    tags=["sensorvacio"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


def read_datos_sensor_by_variable(db, variable, start_date=None, end_date=None):
    cache_key = f"datos_sensor_{variable}_{start_date}_{end_date}"
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

    if start_date:
        query = query.where(SensorDatos.timestamp >= start_date)
    if end_date:
        query = query.where(SensorDatos.timestamp <= end_date)

    resultados = db.execute(query).fetchall()
    datos = [{"time": r.time, "value": r.value, "equipo": r.equipo} for r in resultados]

    import random

    huecos_totales = random.randint(1, 4)  # Number of gaps
    huecos_posiciones = set()
    huecos_info = []
    sigma = (len(resultados) / 100)*0.25
    for _ in range(huecos_totales):
        long_hueco = min(len(resultados) / 100 + sigma, max(len(resultados) / 100 - sigma, int(random.gauss(len(resultados) / 100, sigma))))  # Length of each gap
        while True:
            pos = int(random.randint(0, len(resultados) - long_hueco))
            if not any(pos + i in huecos_posiciones for i in range(long_hueco)):
                huecos_posiciones.update(range(pos, pos + long_hueco))
                huecos_info.append((pos, long_hueco, datos[pos]['time']))
                break

    for pos, length, time in huecos_info:
        print(f"Hueco en la posición: {pos} de {length} de largo, empezando en {time}")

    datos_with_gaps = [dato for i, dato in enumerate(datos) if i not in huecos_posiciones]

    set_cached_response(cache_key, datos_with_gaps)
    return datos_with_gaps


@router.get("/")
def datos_condicionales_sensor(
        variable: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        db: Session = Depends(get_db)
):
    if variable:
        return read_datos_sensor_by_variable(db, variable, start_date, end_date)
    else:
        raise HTTPException(status_code=404, detail="No existe esa combinación.")
