from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select
from alchemy.connector import get_db
from alchemy.models import *

app = FastAPI()

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