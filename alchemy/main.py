from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select
from alchemy.connector import get_db
from alchemy.models import *

app = FastAPI()


@app.get("/")
def read_root():
    return {"message": "Bienvenido a mi API"}


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
                Receptor.timestamp,
                Elemento.simbolo,
                Receptor.valor
            )
            .join(Relacion, Receptor.id == Relacion.id_receptor)
            .join(Elemento, Elemento.id == Relacion.id_elemento)
        )
        resultados = db.execute(query).fetchall()

        datos = [
            {"id": r.id, "timestamp": r.timestamp, "simbolo": r.simbolo, "valor": r.valor}
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
