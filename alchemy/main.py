from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from alchemy.connector import get_db
from alchemy.models import *
from alchemy.redis_client import RedisClient, json_deserializer, json_serializer
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
            Datos.timestamp.label('time'),
            Datos.valor.label('value'),
            Equipo.descripcion.label('equipo')
        )
        .join(Relacion, (Datos.id_equipo == Relacion.id_equipo) & (Datos.id_variable == Relacion.id_variable))
        .join(Variable, Relacion.id_variable == Variable.id)
        .join(Equipo, Relacion.id_equipo == Equipo.id)
        .where(Variable.descripcion == variable_desc)
        .order_by(Datos.timestamp.asc())
    )
    resultados = db.execute(query).fetchall()
    datos = [{"time": r.time, "value": r.value, "equipo": r.equipo} for r in resultados]

    set_cached_response(cache_key, datos)
    return datos


@app.get("/variables/")
def read_elementos(db: Session = Depends(get_db)):
    variables = db.execute(select(Variable)).scalars().all()
    return variables


@app.get("/equipos/")
def read_receptores(db: Session = Depends(get_db)):
    equipos = db.execute(select(Equipo)).scalars().all()
    return equipos


@app.get("/relaciones/")
def read_relaciones(db: Session = Depends(get_db)):
    relaciones = db.execute(select(Relacion)).scalars().all()
    return relaciones


@app.get("/datos/")
def read_datos_WIP(db: Session = Depends(get_db)):
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



@app.get("/datos/oxigeno_disuelto/")
def read_oxigeno_disuelto(db: Session = Depends(get_db)):
    return read_datos_by_variable(db, 'Oxígeno Disuelto')


@app.get("/datos/amonio/")
def read_amonio(db: Session = Depends(get_db)):
    return read_datos_by_variable(db, 'Amonio')


@app.get("/datos/nitrato/")
def read_nitrato(db: Session = Depends(get_db)):
    return read_datos_by_variable(db, 'Nitrato')


@app.get("/datos/solidos_suspendidos_totales/")
def read_solidos_suspendidos_totales(db: Session = Depends(get_db)):
    return read_datos_by_variable(db, 'Sólidos Suspendidos Totales')


@app.get("/datos/solidos_suspendidos_totales_maxmin/")
def read_solidos_suspendidos_totales_max_min(db: Session = Depends(get_db)):
    try:
        # Subconsulta para obtener min y max valores por timestamp
        min_max_subquery = (
            select(
                Datos.timestamp,
                func.min(Datos.valor).label('min_valor'),
                func.max(Datos.valor).label('max_valor')
            )
            .group_by(Datos.timestamp)
            .subquery()
        )

        # Consulta principal uniendo con la subconsulta para obtener los valores originales
        query = (
            select(
                Datos.timestamp,
                Datos.valor,
                min_max_subquery.c.min_valor,
                min_max_subquery.c.max_valor
            )
            .join(min_max_subquery, min_max_subquery.c.timestamp == Datos.timestamp)
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
                func.avg(Datos.valor).label('average_value')
            )
            .join(Relacion, (Datos.id_equipo == Relacion.id_equipo) & (Datos.id_variable == Relacion.id_variable))
            .join(Variable, Relacion.id_variable == Variable.id)
            .group_by(Variable.descripcion)
        )
        resultados = db.execute(query).fetchall()
        datos = [{"metric": r.metric, "average_value": r.average_value} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/datos/ultimos_valores/")
def read_ultimos_valores_WIP(db: Session = Depends(get_db)):
    try:
        subquery = (
            select(
                Datos.id,
                func.max(Datos.timestamp).label('latest_timestamp')
            )
            .group_by(Datos.id)
            .subquery()
        )

        query = (
            select(
                Variable.descripcion.label('metric'),
                Datos.valor.label('latest_value'),
                Datos.timestamp.label('time')
            )
            .join(Relacion, (Datos.id_equipo == Relacion.id_equipo) & (Datos.id_variable == Relacion.id_variable))
            .join(Variable, Relacion.id_variable == Variable.id)
            .join(subquery, (Datos.id == subquery.c.id) & (Datos.timestamp == subquery.c.latest_timestamp))
            .order_by(Variable.descripcion.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"metric": r.metric, "latest_value": r.latest_value, "time": r.time} for r in resultados]
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
                Datos.timestamp,
                Datos.valor
            )
            .join(Relacion, (Datos.id_equipo == Relacion.id_equipo) & (Datos.id_variable == Relacion.id_variable))
            .join(Variable, Relacion.id_variable == Variable.id)
            .filter(Variable.simbolo == metric)
            .filter(Datos.timestamp >= desde)
            .filter(Datos.timestamp <= hasta)
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

