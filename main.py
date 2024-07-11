from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, literal, extract
from connector import get_db
from models import *
from redis_client import RedisClient, json_deserializer, json_serializer
import json

app = FastAPI()

# Inicializa el cliente de Redis
redis_client = RedisClient().get_client()


@app.get("/")
def read_root():
    return {"HolaMundo": "Bienvenido a mi API"}


def get_cached_response(key):
    cached_data = redis_client.get(key)
    if cached_data:
        print(f"Data retrieved from Redis for key: {key}")  # Aviso en consola
        return json.loads(cached_data, object_hook=json_deserializer)
    return None


def set_cached_response(key, data, expiration=60):
    print(f"Data sent to Redis for key: {key}")  # Aviso en consola
    redis_client.setex(key, expiration, json.dumps(data, default=json_serializer))


def read_datos_by_variable(db, variable_desc):
    cache_key = f"datos_{variable_desc}"
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
        .where(Variable.descripcion == variable_desc)
        .order_by(SensorDatos.timestamp.asc())
    )
    resultados = db.execute(query).fetchall()
    datos = [{"time": r.time, "value": r.value, "equipo": r.equipo} for r in resultados]

    set_cached_response(cache_key, datos)
    return datos


@app.get("/variables/")
def read_variables(db: Session = Depends(get_db)):
    query = select(Variable).order_by(Variable.id)
    variables = db.execute(query).scalars().all()
    return variables


@app.get("/equipos/")
def read_equipos(db: Session = Depends(get_db)):
    equipos = db.execute(select(Equipo)).scalars().all()
    return equipos


@app.get("/relaciones/")
def read_relaciones(db: Session = Depends(get_db)):
    relaciones = db.execute(select(Sensor)).scalars().all()
    return relaciones

'''
@app.get("/datos/")
def read_datos_wip(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Datos.id,
                Equipo.nombre.label('equipo_nombre'),
                Variable.descripcion.label('variable_descripcion'),
                Datos.timestamp,
                Datos.valor
            )
            .join(Relacion, (Datos.id_equipo == Relacion.id_equipo) & (Datos.id_variable == Relacion.id_variable))
            .join(Equipo, Relacion.id_equipo == Equipo.id)
            .join(Variable, Relacion.id_variable == Variable.id)
            .order_by(Datos.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()

        datos = [
            {"id": r.id, "equipo_nombre": r.equipo_nombre, "variable_descripcion": r.variable_descripcion,
             "timestamp": r.timestamp, "valor": r.valor}
            for r in resultados
        ]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")
'''

@app.get("/datos/oxigeno_disuelto/")
def read_oxigeno_disuelto(db: Session = Depends(get_db)):
    return read_datos_by_variable(db, 'Oxígeno Disuelto')


@app.get("/datos/amonio/")
def read_amonio(db: Session = Depends(get_db)):
    return read_datos_by_variable(db, 'Amonio')


@app.get("/datos/nitrato/")
def read_nitrato(db: Session = Depends(get_db)):
    return read_datos_by_variable(db, 'Nitrato')


@app.get("/datos/temperatura/")
def read_temperatura(db: Session = Depends(get_db)):
    return read_datos_by_variable(db, 'Temperatura')


@app.get("/datos/caudal_aire/")
def read_caudal_aire(db: Session = Depends(get_db)):
    return read_datos_by_variable(db, 'Caudal de aire')


@app.get("/datos/caudal_agua/")
def read_caudal_agua(db: Session = Depends(get_db)):
    return read_datos_by_variable(db, 'Caudal de agua')


@app.get("/datos/solidos_suspendidos_totales/")
def read_solidos_suspendidos_totales(db: Session = Depends(get_db)):
    return read_datos_by_variable(db, 'Sólidos Suspendidos Totales')


@app.get("/datos/solidos_suspendidos_totales_maxmin/")
def read_solidos_suspendidos_totales_max_min(db: Session = Depends(get_db)):
    try:
        # Subconsulta para obtener min y max valores por timestamp
        min_max_subquery = (
            select(
                SensorDatos.timestamp,
                func.min(SensorDatos.valor).label('min_valor'),
                func.max(SensorDatos.valor).label('max_valor')
            )
            .group_by(SensorDatos.timestamp)
            .subquery()
        )

        # Consulta principal uniendo con la subconsulta para obtener los valores originales
        query = (
            select(
                SensorDatos.timestamp,
                SensorDatos.valor,
                min_max_subquery.c.min_valor,
                min_max_subquery.c.max_valor
            )
            .join(min_max_subquery, min_max_subquery.c.timestamp == SensorDatos.timestamp)
        )

        resultados = db.execute(query).fetchall()
        datos = [
            {"timestamp": r.timestamp, "valor": r.valor, "min_valor": r.min_valor, "max_valor": r.max_valor}
            for r in resultados
        ]
        return datos

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la consulta: {str(e)}")


@app.get("/datos/promedio_valores/")
def read_promedio_valores(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Variable.descripcion.label('metric'),
                func.avg(SensorDatos.valor).label('average_value'),
                func.concat(Equipo.nombre, literal(', ('), Variable.u_medida, literal(')')).label('equipo')
            )
            .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
            .join(Variable, Sensor.id_variable == Variable.id)
            .join(Equipo, Sensor.id_equipo == Equipo.id)
            .where(Variable.descripcion.in_(['Amonio', 'Nitrato', 'Oxígeno Disuelto', 'Sólidos Suspendidos Totales']))
            .group_by(Equipo.nombre, Variable.u_medida, Variable.descripcion)
        )
        resultados = db.execute(query).fetchall()
        datos = [{"metric": r.metric, "average_value": r.average_value, "equipo": r.equipo} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/datos/promedio_valores_mes/")
def read_promedio_valores_mes(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Variable.descripcion.label('metric'),
                func.avg(SensorDatos.valor).label('average_value'),
                func.concat(Equipo.nombre, literal(', ('), Variable.u_medida, literal(')')).label('equipo'),
                extract('year', SensorDatos.timestamp).label('year'),
                extract('month', SensorDatos.timestamp).label('month')
            )
            .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
            .join(Variable, Sensor.id_variable == Variable.id)
            .join(Equipo, Sensor.id_equipo == Equipo.id)
            .where(Variable.descripcion.in_(['Amonio', 'Nitrato', 'Oxígeno Disuelto', 'Sólidos Suspendidos Totales']))
            .group_by(Equipo.nombre, Variable.u_medida, Variable.descripcion, 'year', 'month')
        )
        resultados = db.execute(query).fetchall()
        datos = [{"metric": r.metric, "average_value": r.average_value, "equipo": r.equipo, "year": r.year, "month": r.month} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/datos/promedio_valores_grandes/")
def read_promedio_valores_grandes(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Variable.descripcion.label('metric'),
                func.avg(SensorDatos.valor).label('average_value'),
                func.concat(Equipo.nombre, literal(', ('), Variable.u_medida, literal(')')).label('equipo')
            )
            .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
            .join(Variable, Sensor.id_variable == Variable.id)
            .join(Equipo, Sensor.id_equipo == Equipo.id)
            .where(Variable.descripcion.in_(['Caudal de aire', 'Caudal de agua', 'Temperatura']))
            .group_by(Equipo.nombre, Variable.u_medida, Variable.descripcion)
        )
        resultados = db.execute(query).fetchall()
        datos = [{"metric": r.metric, "average_value": r.average_value, "equipo": r.equipo} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/datos/promedio_valores_grandes_mes/")
def read_promedio_valores_grandes(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Variable.descripcion.label('metric'),
                func.avg(SensorDatos.valor).label('average_value'),
                func.concat(Equipo.nombre, literal(', ('), Variable.u_medida, literal(')')).label('equipo'),
                extract('year', SensorDatos.timestamp).label('year'),
                extract('month', SensorDatos.timestamp).label('month')
            )
            .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
            .join(Variable, Sensor.id_variable == Variable.id)
            .join(Equipo, Sensor.id_equipo == Equipo.id)
            .where(Variable.descripcion.in_(['Caudal de aire', 'Caudal de agua', 'Temperatura']))
            .group_by(Equipo.nombre, Variable.u_medida, Variable.descripcion, 'year', 'month')
        )
        resultados = db.execute(query).fetchall()
        datos = [{"metric": r.metric, "average_value": r.average_value, "equipo": r.equipo, "year": r.year, "month": r.month} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/datos/ultimos_valores/")
def read_ultimos_valores_wip(db: Session = Depends(get_db)):
    try:
        subquery = (
            select(
                SensorDatos.id,
                func.max(SensorDatos.timestamp).label('latest_timestamp')
            )
            .group_by(SensorDatos.id)
            .subquery()
        )

        query = (
            select(
                Variable.descripcion.label('metric'),
                SensorDatos.valor.label('latest_value'),
                SensorDatos.timestamp.label('time')
            )
            .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
            .join(Variable, Sensor.id_variable == Variable.id)
            .join(subquery, (SensorDatos.id == subquery.c.id) & (SensorDatos.timestamp == subquery.c.latest_timestamp))
            .order_by(Variable.descripcion.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"metric": r.metric, "latest_value": r.latest_value, "time": r.time} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/datos/valores_consigna/")
def read_valores_consigna(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Consigna.nombre.label('consigna'),
                ValoresConsigna.valor.label('valor'),
                ValoresConsigna.timestamp.label('time'),
                ValoresConsigna.mode.label('mode')
            )
            .join(Consigna, ValoresConsigna.id_consigna == Consigna.id)
            .order_by(ValoresConsigna.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"consigna": r.consigna, "valor": r.valor, "time": r.time, "mode": r.mode} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/datos/datos_filtrados/")
def read_datos_filtrados(db: Session = Depends(get_db)):
    try:
        query=(
            select(
                Senal.nombre.label('senal'),
                SenalDatos.valor.label('valor'),
                SenalDatos.timestamp.label('time')
            )
            .join(Senal, SenalDatos.id_señal == Senal.id)
            .order_by(SenalDatos.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"senal": r.senal, "valor": r.valor, "time": r.time} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/metrics")
def get_metrics_metadata(db: Session = Depends(get_db)):
    try:
        variables = db.execute(select(Variable)).scalars().all()
        equipos = db.execute(select(Equipo)).scalars().all()

        # Formatear los elementos y receptores en un formato compatible con Grafana
        variables_metadata = [{"text": variable.descripcion, "value": variable.id} for variable in variables]
        equipos_metadata = [{"text": equipo.nombre, "value": equipo.id} for equipo in equipos]

        # Combinar elementos y receptores en un solo objeto
        metadata = {
            "variables": variables_metadata,
            "equipos": equipos_metadata
        }

        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener metadatos: {str(e)}")


@app.get("/query")
def query_data(metric: str, desde: int, hasta: int, db: Session = Depends(get_db)):
    try:
        # Aquí debes construir la consulta según los parámetros recibidos
        query = (
            select(
                SensorDatos.timestamp,
                SensorDatos.valor
            )
            .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
            .join(Variable, Sensor.id_variable == Variable.id)
            .filter(Variable.simbolo.__eq__(metric))
            .filter(SensorDatos.timestamp >= desde)
            .filter(SensorDatos.timestamp <= hasta)
        )
        resultados = db.execute(query).fetchall()

        # Formatear los resultados en un formato compatible con Grafana
        data_points = [{"timestamp": r.timestamp, "value": r.valor} for r in resultados]

        response = {
            "target": metric,
            "datapoints": data_points
        }

        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta: {str(e)}")
