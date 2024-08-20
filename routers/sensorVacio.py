from collections import defaultdict
from typing import Optional
from fastapi import Depends, HTTPException, APIRouter, status
from sqlalchemy.orm import Session
from sqlalchemy import select, extract, func
from db.connector import get_db
from db.models import *
from db.redis_client import set_cached_response, get_cached_response
from utils.agregacion import *
from datetime import datetime
from utils.date_checker import date_validator
from utils.gap_generator import generar_huecos

router = APIRouter(
    prefix="/datos/sensorvacio",
    tags=["sensorvacio"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


def read_datos_sensor_by_variable(db, variable, equipo, start_date=None, end_date=None, tipo=None):
    cache_key = f"datos_sensor_{variable}_{equipo}_{start_date}_{end_date}_{tipo}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

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

    # Gestión de fechas de la consulta
    query = date_validator(query, end_date, start_date)

    # Ejecutar la consulta y obtener los resultados
    resultados = db.execute(query).fetchall()
    datos = [{"time": r.time, "value": r.value, "equipo": r.equipo} for r in resultados]

    nombre_equipo = datos[0]['equipo']
    deltat = resultados[0].deltat

    datos_with_gaps, huecos_info = generar_huecos(datos)

    datos_finales = agregacion(datos, datos_with_gaps, deltat, huecos_info, nombre_equipo, tipo)

    set_cached_response(cache_key, datos_finales)

    return datos_finales


def agregacion(datos, datos_with_gaps, deltat, huecos_info, nombre_equipo, tipo):
    s_data = [dato['value'] for dato in datos_with_gaps]
    s_time = [int(dato['time'].timestamp() * 1000) for dato in datos_with_gaps]
    z = calcular_delta_prima(tipo, deltat, [datos_with_gaps[0]['time'], datos_with_gaps[-1]['time']])
    if z == deltat:
        for pos, length, time in huecos_info:
            for i in range(pos, pos + length):
                datos_with_gaps.insert(i, {"time": datos[i]['time'], "value": None, "equipo": datos[i]['equipo']})
        return datos_with_gaps
    datos_agregados = get_datos_sin_hueco([datos_with_gaps[0]['time'], datos_with_gaps[-1]['time']], s_data, s_time, z)
    datos_finales = []
    for item in datos_agregados:
        datos_finales.append({
            "time": item[0].isoformat(),  # Convertir a string ISO 8601
            "value": item[1],
            "equipo": nombre_equipo
        })
    return datos_finales


@router.get("/")
def datos_condicionales_sensor(
        variable: Optional[str] = None,
        equipo: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        tipo: Optional[str] = None,
        db: Session = Depends(get_db)
):
    if variable and equipo:
        try:
            return read_datos_sensor_by_variable(db, variable, equipo, start_date, end_date, tipo)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except IndexError:
            raise HTTPException(status_code=404, detail="No hay datos suficientes.")
    elif variable and not equipo:
        raise HTTPException(status_code=400, detail="Falta el nombre del equipo.")
    else:
        raise HTTPException(status_code=404, detail="No existe esa combinación.")


@router.get("/heatmap")
def datos_heatmap_sensor(db: Session = Depends(get_db), variable: Optional[str] = None, equipo: Optional[str] = None,
                         year: Optional[int] = None):
    cache_key = f"heatmap_sensor_{variable}_{equipo}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    if year is None:
        year = datetime.now().year

    query = (
        select(
            func.week(SensorDatos.timestamp, 1).label('week'),
            func.dayname(SensorDatos.timestamp).label('day'),
            func.avg(SensorDatos.valor).label('average_value')
        )
        .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
        .join(Variable, Sensor.id_variable == Variable.id)
        .join(Equipo, Sensor.id_equipo == Equipo.id)
        .where(Variable.simbolo == variable)
        .where(Equipo.nombre == equipo)
        .where(extract('year', SensorDatos.timestamp) == year)
        .group_by(
            func.week(SensorDatos.timestamp, 1),
            func.dayname(SensorDatos.timestamp),
            func.dayofweek(SensorDatos.timestamp)
        )
        .order_by(
            func.week(SensorDatos.timestamp, 1),
            func.dayofweek(SensorDatos.timestamp)
        )
    )

    resultados = db.execute(query).fetchall()

    weekly_data = {}
    days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    for r in resultados:
        week = r.week
        day = r.day
        avg_value = r.average_value

        if week not in weekly_data:
            weekly_data[week] = {day: None for day in days_of_week}

        weekly_data[week][day] = avg_value

    formatted_result = [{"Week": week, **data} for week, data in sorted(weekly_data.items())]

    return formatted_result
