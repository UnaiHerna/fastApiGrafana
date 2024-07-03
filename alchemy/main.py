from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from alchemy.connector import get_db
from alchemy.models import *

app = FastAPI()


@app.get("/")
def read_root():
    return {"HolaMundo": "Bienvenido a mi API"}


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
def read_datos(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Dato.id,
                Equipo.nombre.label('equipo_nombre'),
                Variable.descripcion.label('variable_descripcion'),
                Dato.timestamp,
                Dato.valor
            )
            .join(Relacion, (Dato.id_equipo == Relacion.id_equipo) & (Dato.id_variable == Relacion.id_variable))
            .join(Equipo, Relacion.id_equipo == Equipo.id)
            .join(Variable, Relacion.id_variable == Variable.id)
            .order_by(Dato.timestamp.asc())
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
    try:
        query = (
            select(
                Dato.timestamp.label('time'),
                Dato.valor.label('value')
            )
            .join(Relacion, (Dato.id_equipo == Relacion.id_equipo) & (Dato.id_variable == Relacion.id_variable))
            .join(Variable, Relacion.id_variable == Variable.id)
            .where(Variable.descripcion == 'Oxígeno Disuelto')
            .order_by(Dato.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"time": r.time, "value": r.value} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/datos/amonio/")
def read_amonio(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Dato.timestamp.label('time'),
                Dato.valor.label('value')
            )
            .join(Relacion, (Dato.id_equipo == Relacion.id_equipo) & (Dato.id_variable == Relacion.id_variable))
            .join(Variable, Relacion.id_variable == Variable.id)
            .where(Variable.descripcion == 'Amonio')
            .order_by(Dato.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"time": r.time, "value": r.value} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/datos/nitrato/")
def read_nitrato(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Dato.timestamp.label('time'),
                Dato.valor.label('value')
            )
            .join(Relacion, (Dato.id_equipo == Relacion.id_equipo) & (Dato.id_variable == Relacion.id_variable))
            .join(Variable, Relacion.id_variable == Variable.id)
            .where(Variable.descripcion == 'Nitrato')
            .order_by(Dato.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"time": r.time, "value": r.value} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/datos/solidos_suspendidos_totales/")
def read_solidos_suspendidos_totales(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Dato.timestamp.label('time'),
                Dato.valor.label('value')
            )
            .join(Relacion, (Dato.id_equipo == Relacion.id_equipo) & (Dato.id_variable == Relacion.id_variable))
            .join(Variable, Relacion.id_variable == Variable.id)
            .where(Variable.descripcion == 'Sólidos Suspendidos Totales')
            .order_by(Dato.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"time": r.time, "value": r.value} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/datos/promedio_valores/")
def read_promedio_valores(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Variable.descripcion.label('metric'),
                func.avg(Dato.valor).label('average_value')
            )
            .join(Relacion, (Dato.id_equipo == Relacion.id_equipo) & (Dato.id_variable == Relacion.id_variable))
            .join(Variable, Relacion.id_variable == Variable.id)
            .group_by(Variable.descripcion)
        )
        resultados = db.execute(query).fetchall()
        datos = [{"metric": r.metric, "average_value": r.average_value} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/datos/ultimos_valores/")
def read_ultimos_valores(db: Session = Depends(get_db)):
    try:
        subquery = (
            select(
                Dato.id,
                func.max(Dato.timestamp).label('latest_timestamp')
            )
            .group_by(Dato.id)
            .subquery()
        )

        query = (
            select(
                Variable.descripcion.label('metric'),
                Dato.valor.label('latest_value'),
                Dato.timestamp.label('time')
            )
            .join(Relacion, (Dato.id_equipo == Relacion.id_equipo) & (Dato.id_variable == Relacion.id_variable))
            .join(Variable, Relacion.id_variable == Variable.id)
            .join(subquery, (Dato.id == subquery.c.id) & (Dato.timestamp == subquery.c.latest_timestamp))
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
                Dato.timestamp,
                Dato.valor
            )
            .join(Relacion, (Dato.id_equipo == Relacion.id_equipo) & (Dato.id_variable == Relacion.id_variable))
            .join(Variable, Relacion.id_variable == Variable.id)
            .filter(Variable.simbolo == metric)
            .filter(Dato.timestamp >= desde)
            .filter(Dato.timestamp <= hasta)
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
        raise HTTPException(status_code=500, detail=f"Error al realizar la consulta: {str(e)}")
