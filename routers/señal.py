import datetime
from typing import Optional

from fastapi import Depends, HTTPException, APIRouter, status
from sqlalchemy.orm import Session
from sqlalchemy import select
from db.connector import get_db
from db.models import *
from db.redis_client import set_cached_response, get_cached_response


router = APIRouter(
    prefix="/datos/senal",
    tags=["señal"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


def read_senal_datos_by_nombre(db, senal, start_time=None, end_time=None):
    cache_key = f"datos_senal_{senal}_{start_time}_{end_time}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    query = (
        select(
            SenalDatos.timestamp.label('time'),
            SenalDatos.valor.label('value'),
            Senal.nombre.label('senal')
        )
        .join(Senal, SenalDatos.id_señal == Senal.id)
        .where(Senal.nombre == senal)
        .order_by(SenalDatos.timestamp.asc())
    )
    resultados = db.execute(query).fetchall()
    datos = [{"time": r.time, "value": r.value, "senal": r.senal} for r in resultados]

    set_cached_response(cache_key, datos)
    return datos


def read_senal_multiple_by_nombre(db, nombres, start_time=None, end_time=None):
    senal_list = nombres.split(',')
    all_data = {}
    for senal in senal_list:
        data = read_senal_datos_by_nombre(db, senal, start_time, end_time)
        all_data[senal] = data
    return all_data


@router.get("/")
def datos_condicionales_consigna(
        nombre: Optional[str] = None,
        nombres: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        db: Session = Depends(get_db)
):
    if nombre and not nombres:
        return read_senal_datos_by_nombre(db, nombre, start_time, end_time)
    elif nombres and not nombre:
        return read_senal_multiple_by_nombre(db, nombres, start_time, end_time)
    else:
        # Lógica para manejar la solicitud cuando no se proporciona ninguno de los parámetros esperados
        raise HTTPException(status_code=400, detail="Debe proporcionar los datos de forma correcta.")
