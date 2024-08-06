from typing import Optional
from fastapi import Depends, HTTPException, APIRouter, status
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from db.connector import get_db
from db.models import *
from db.redis_client import set_cached_response, get_cached_response
from utils.agregacion import *
from datetime import datetime

from utils.gap_generator import generar_huecos

router = APIRouter(
    prefix="/datos/sensorvacio",
    tags=["sensorvacio"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


def read_datos_sensor_by_variable(db, variable, equipo, start_date=None, end_date=None):
    cache_key = f"datos_sensor_{variable}_{equipo}_{start_date}_{end_date}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    # Main query to get the data to be returned
    query = (
        select(
            SensorDatos.timestamp.label('time'),
            SensorDatos.valor.label('value'),
            Equipo.descripcion.label('equipo'),
            Sensor.deltat.label('deltat')
        )
        .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
        .join(Variable, Sensor.id_variable == Variable.id)
        .join(Equipo, Sensor.id_equipo == Equipo.id)
        .where(Variable.simbolo == variable)
        .where(Equipo.nombre == equipo)
        .order_by(SensorDatos.timestamp.asc())
    )

    if start_date:
        query = query.where(SensorDatos.timestamp >= start_date)
    if end_date:
        query = query.where(SensorDatos.timestamp <= end_date)

    # Obtener la fecha y hora actuales
    current_datetime = datetime.now()
    query = query.where(SensorDatos.timestamp <= current_datetime)

    # Ejecutar la consulta y obtener los resultados
    resultados = db.execute(query).fetchall()
    datos = [{"time": r.time, "value": r.value, "equipo": r.equipo} for r in resultados]

    nombre_equipo = datos[0]['equipo']
    deltat = resultados[0].deltat

    datos_with_gaps, huecos_info = generar_huecos(datos)

    set_cached_response(cache_key, datos_with_gaps)

    s_data = [dato['value'] for dato in datos_with_gaps]
    s_time = [int(dato['time'].timestamp() * 1000) for dato in datos_with_gaps]

    z = calcular_delta_prima(deltat, [datos_with_gaps[0]['time'], datos_with_gaps[-1]['time']])

    if z == deltat:
        for pos, length, time in huecos_info:
            for i in range(pos, pos + length):
                datos_with_gaps.insert(i, {"time": datos[i]['time'], "value": None, "equipo": datos[i]['equipo']})
        return datos_with_gaps

    datos_agregados = get_datos_sin_hueco([datos_with_gaps[0]['time'], datos_with_gaps[-1]['time']], s_data, s_time, z)

    datos = []
    for item in datos_agregados:
        datos.append({
            "time": item[0].isoformat(), #Pasarlo a string/datetime
            "value": item[1],
            "equipo": nombre_equipo
        })

    return datos


@router.get("/")
def datos_condicionales_sensor(
        variable: Optional[str] = None,
        equipo: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        db: Session = Depends(get_db)
):
    if variable and equipo:
        return read_datos_sensor_by_variable(db, variable, equipo, start_date, end_date)
    elif variable and not equipo:
        raise HTTPException(status_code=400, detail="Falta el nombre del equipo.")
    else:
        raise HTTPException(status_code=404, detail="No existe esa combinación.")
