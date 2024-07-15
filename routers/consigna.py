from typing import Optional

from fastapi import Depends, HTTPException, APIRouter, status
from sqlalchemy.orm import Session
from sqlalchemy import select
from db.connector import get_db
from db.models import *
from db.redis_client import set_cached_response, get_cached_response


router = APIRouter(
    prefix="/datos/consigna",
    tags=["consigna"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


def read_datos_consigna_by_nombre(db, consigna):
    cache_key = f"datos_consigna_{consigna}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    query = (
        select(
            Consigna.nombre.label('consigna'),
            ValoresConsigna.valor.label('valor'),
            ValoresConsigna.timestamp.label('time'),
            ValoresConsigna.mode.label('mode')
        )
        .join(Consigna, ValoresConsigna.id_consigna == Consigna.id)
        .where(Consigna.nombre == consigna)
        .order_by(ValoresConsigna.timestamp.asc())
    )
    resultados = db.execute(query).fetchall()
    datos = [{"time": r.time, "value": r.value, "mode": r.mode, "consigna": r.consigna} for r in resultados]

    set_cached_response(cache_key, datos)
    return datos


def read_datos_consigna_by_equipo(db, equipo):
    cache_key = f"datos_consigna_{equipo}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    query = (
        select(
            Consigna.nombre.label('consigna'),
            ValoresConsigna.valor.label('valor'),
            ValoresConsigna.timestamp.label('time'),
            ValoresConsigna.mode.label('mode')
        )
        .join(Consigna, ValoresConsigna.id_consigna == Consigna.id)
        .join(Equipo, Consigna.id_equipo == Equipo.id)
        .where(Equipo.nombre == equipo)
        .order_by(ValoresConsigna.timestamp.asc())
    )
    resultados = db.execute(query).fetchall()
    datos = [{"time": r.time, "value": r.value, "mode": r.mode, "consigna": r.consigna} for r in resultados]

    set_cached_response(cache_key, datos)
    return datos


def read_consigna_multiple_by_nombre(db, nombres):
    consigna_list = nombres.split(',')
    all_data = {}
    for consigna in consigna_list:
        data = read_datos_consigna_by_nombre(db, consigna)
        all_data[consigna] = data
    return all_data


def read_consigna_multiple_by_equipo(db, nombres):
    equipo_list = nombres.split(',')
    all_data = {}
    for equipo in equipo_list:
        data = read_datos_consigna_by_equipo(db, equipo)
        all_data[equipo] = data
    return all_data


@router.get("/")
def datos_condicionales_consigna(
        nombre: Optional[str] = None,
        nombres: Optional[str] = None,
        equipo: Optional[str] = None,
        equipos: Optional[str] = None,
        db: Session = Depends(get_db)
):
    if nombre and not equipo and not nombres and not equipos:
        return read_datos_consigna_by_nombre(db, nombre)
    elif equipo and not nombre and not nombres and not equipos:
        return read_datos_consigna_by_equipo(db, equipo)
    elif nombres and not equipo and not nombre and not equipos:
        return read_consigna_multiple_by_nombre(db, nombres)
    elif equipos and not equipo and not nombre and not nombres:
        return read_consigna_multiple_by_equipo(db, equipos)
    else:
        # Lógica para manejar la solicitud cuando no se proporciona ninguno de los parámetros esperados
        raise HTTPException(status_code=400, detail="Debe proporcionar los datos de forma correcta.")
