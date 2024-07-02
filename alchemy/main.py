from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from alchemy.connector import get_db
from alchemy.models import *

app = FastAPI()


@app.get("/")
def read_root():
    return {"HolaMundo": "Bienvenido a mi API"}


@app.get("/elementos/")
def read_elementos(db: Session = Depends(get_db)):
    elementos = db.execute(select(Elemento)).scalars().all()
    return elementos


@app.get("/receptores/")
def read_receptores(db: Session = Depends(get_db)):
    receptores = db.execute(select(Receptor)).scalars().all()
    return receptores


@app.get("/relaciones/")
def read_relaciones(db: Session = Depends(get_db)):
    relaciones = db.execute(select(Relacion)).scalars().all()
    return relaciones


@app.get("/elementos/{elemento_id}")
def read_elemento(elemento_id: int, db: Session = Depends(get_db)):
    elemento = db.execute(select(Elemento).filter(Elemento.id == elemento_id)).scalars().first()
    if elemento is None:
        raise HTTPException(status_code=404, detail="Elemento not found")
    return elemento


@app.get("/receptores/{receptor_id}")
def read_receptor(receptor_id: int, db: Session = Depends(get_db)):
    receptor = db.execute(select(Receptor).filter(Receptor.id == receptor_id)).scalars().first()
    if receptor is None:
        raise HTTPException(status_code=404, detail="Receptor not found")
    return receptor


@app.get("/datos/")
def read_datos(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Receptor.id,
                Receptor.nombre,
                Receptor.timestamp,
                Elemento.simbolo,
                Receptor.valor
            )
            .join(Relacion, Receptor.id == Relacion.id_receptor)
            .join(Elemento, Elemento.id == Relacion.id_elemento)
            .order_by(Receptor.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()

        datos = [
            {"id": r.id, "nombre": r.nombre, "timestamp": r.timestamp, "simbolo": r.simbolo, "valor": r.valor}
            for r in resultados
        ]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


@app.get("/metrics")
def get_metrics_metadata(db: Session = Depends(get_db)):
    try:
        elementos = db.execute(select(Elemento)).scalars().all()
        receptores = db.execute(select(Receptor)).scalars().all()

        # Formatear los elementos y receptores en un formato compatible con Grafana
        elements_metadata = [{"text": elemento.simbolo, "value": elemento.id} for elemento in elementos]
        receivers_metadata = [{"text": receptor.id, "value": receptor.id} for receptor in receptores]

        # Combinar elementos y receptores en un solo objeto
        metadata = {
            "elements": elements_metadata,
            "receivers": receivers_metadata
        }

        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener metadatos: {str(e)}")


@app.get("/query")
def query_data(metric: str, desde: int, hasta: int, db: Session = Depends(get_db)):
    try:
        # Aquí debes construir la consulta según los parámetros recibidos
        # Ejemplo de construcción de la consulta basada en el ejemplo anterior
        query = (
            select(
                Receptor.timestamp,
                Receptor.valor
            )
            .join(Relacion, Receptor.id == Relacion.id_receptor)
            .join(Elemento, Elemento.id == Relacion.id_elemento)
            .filter(Elemento.simbolo == metric)
            .filter(Receptor.timestamp >= desde)
            .filter(Receptor.timestamp <= hasta)
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


## Consulta 1: Valores de Oxígeno Disuelto a lo largo del tiempo
@app.get("/datos/oxigeno_disuelto/")
def read_oxigeno_disuelto(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Receptor.timestamp.label('time'),
                Receptor.valor.label('value'),
                Elemento.descripcion.label('metric')
            )
            .join(Relacion, Receptor.id == Relacion.id_receptor)
            .join(Elemento, Elemento.id == Relacion.id_elemento)
            .where(Elemento.descripcion == 'Oxígeno Disuelto')
            .order_by(Receptor.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"time": r.time, "value": r.value, "metric": r.metric} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")

# Consulta 2: Valores de Amonio a lo largo del tiempo
@app.get("/datos/amonio/")
def read_amonio(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Receptor.timestamp.label('time'),
                Receptor.valor.label('value'),
                Elemento.descripcion.label('metric')
            )
            .join(Relacion, Receptor.id == Relacion.id_receptor)
            .join(Elemento, Elemento.id == Relacion.id_elemento)
            .where(Elemento.descripcion == 'Amonio')
            .order_by(Receptor.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"time": r.time, "value": r.value, "metric": r.metric} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")

# Consulta 3: Valores de Nitrato a lo largo del tiempo
@app.get("/datos/nitrato/")
def read_nitrato(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Receptor.timestamp.label('time'),
                Receptor.valor.label('value'),
                Elemento.descripcion.label('metric')
            )
            .join(Relacion, Receptor.id == Relacion.id_receptor)
            .join(Elemento, Elemento.id == Relacion.id_elemento)
            .where(Elemento.descripcion == 'Nitrato')
            .order_by(Receptor.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"time": r.time, "value": r.value, "metric": r.metric} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")

# Consulta 4: Valores de Sólidos Suspendidos Totales a lo largo del tiempo
@app.get("/datos/solidos_suspendidos_totales/")
def read_solidos_suspendidos_totales(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Receptor.timestamp.label('time'),
                Receptor.valor.label('value'),
                Elemento.descripcion.label('metric')
            )
            .join(Relacion, Receptor.id == Relacion.id_receptor)
            .join(Elemento, Elemento.id == Relacion.id_elemento)
            .where(Elemento.descripcion == 'Sólidos Suspendidos Totales')
            .order_by(Receptor.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"time": r.time, "value": r.value, "metric": r.metric} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


# Consulta 5: Promedio de Valores por Tipo de Elemento
@app.get("/datos/promedio_valores/")
def read_promedio_valores(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Elemento.descripcion.label('metric'),
                func.avg(Receptor.valor).label('average_value')
            )
            .join(Relacion, Receptor.id == Relacion.id_receptor)
            .join(Elemento, Elemento.id == Relacion.id_elemento)
            .group_by(Elemento.descripcion)
        )
        resultados = db.execute(query).fetchall()
        datos = [{"metric": r.metric, "average_value": r.average_value} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")


# Consulta 6: Últimos Valores Registrados por Tipo de Elemento
@app.get("/datos/ultimos_valores/")
def read_ultimos_valores(db: Session = Depends(get_db)):
    try:
        subquery = (
            select(
                Receptor.id,
                func.max(Receptor.timestamp).label('latest_timestamp')
            )
            .group_by(Receptor.id)
            .subquery()
        )

        query = (
            select(
                Elemento.descripcion.label('metric'),
                Receptor.valor.label('latest_value'),
                Receptor.timestamp.label('time')
            )
            .join(Relacion, Receptor.id == Relacion.id_receptor)
            .join(Elemento, Elemento.id == Relacion.id_elemento)
            .join(subquery, (Receptor.id == subquery.c.id) & (Receptor.timestamp == subquery.c.latest_timestamp))
            .order_by(Elemento.descripcion.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"metric": r.metric, "latest_value": r.latest_value, "time": r.time} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")