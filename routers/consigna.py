from typing import Optional
from fastapi import Depends, HTTPException, APIRouter, status
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from db.connector import get_db
from db.models import *
from db.redis_client import set_cached_response, get_cached_response
from datetime import datetime

router = APIRouter(
    prefix="/datos/consigna",
    tags=["consigna"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


def read_datos_consigna_by_nombre(db, consigna, start_date=None, end_date=None):
    cache_key = f"datos_consigna_{consigna}_{start_date}_{end_date}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    query = (
        select(
            Consigna.nombre.label('consigna'),
            ValoresConsigna.valor.label('value'),
            ValoresConsigna.timestamp.label('time'),
            ValoresConsigna.mode.label('mode')
        )
        .join(Consigna, ValoresConsigna.id_consigna == Consigna.id)
        .where(Consigna.nombre == consigna)
        .order_by(ValoresConsigna.timestamp.asc())
    )

    if start_date:
        query = query.where(ValoresConsigna.timestamp >= start_date)
    if end_date:
        query = query.where(ValoresConsigna.timestamp <= end_date)

    resultados = db.execute(query).fetchall()
    datos = [{"time": r.time, "value": r.value, "mode": r.mode, "consigna": r.consigna} for r in resultados]

    set_cached_response(cache_key, datos)
    return datos


def read_datos_consigna_by_equipo(db, equipo, start_date=None, end_date=None):
    cache_key = f"datos_consigna_{equipo}_{start_date}_{end_date}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    query = (
        select(
            Consigna.nombre.label('consigna'),
            ValoresConsigna.valor.label('value'),
            ValoresConsigna.timestamp.label('time'),
            ValoresConsigna.mode.label('mode')
        )
        .join(Consigna, ValoresConsigna.id_consigna == Consigna.id)
        .join(Equipo, Consigna.id_equipo == Equipo.id)
        .where(Equipo.nombre == equipo)
        .order_by(ValoresConsigna.timestamp.asc())
    )

    if start_date:
        query = query.where(ValoresConsigna.timestamp >= start_date)
    if end_date:
        query = query.where(ValoresConsigna.timestamp <= end_date)

    resultados = db.execute(query).fetchall()
    datos = [{"time": r.time, "value": r.value, "mode": r.mode, "consigna":  r.consigna} for r in resultados]

    set_cached_response(cache_key, datos)
    return datos


def read_consigna_multiple_by_nombre(db, nombres, start_date=None, end_date=None):
    consigna_list = nombres.split(',')
    all_data = {}
    for consigna in consigna_list:
        data = read_datos_consigna_by_nombre(db, consigna, start_date, end_date)
        all_data[consigna] = data
    return all_data


def read_consigna_multiple_by_equipo(db, equipos, start_date=None, end_date=None):
    equipo_list = equipos.split(',')
    all_data = {}
    for equipo in equipo_list:
        data = read_datos_consigna_by_equipo(db, equipo, start_date, end_date)
        all_data[equipo] = data
    return all_data


@router.get("/")
def datos_condicionales_consigna(
        nombre: Optional[str] = None,
        nombres: Optional[str] = None,
        equipo: Optional[str] = None,
        equipos: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        db: Session = Depends(get_db)
):
    if nombre and not equipo and not nombres and not equipos:
        return read_datos_consigna_by_nombre(db, nombre, start_date, end_date)
    elif equipo and not nombre and not nombres and not equipos:
        return read_datos_consigna_by_equipo(db, equipo, start_date, end_date)
    elif nombres and not equipo and not nombre and not equipos:
        return read_consigna_multiple_by_nombre(db, nombres, start_date, end_date)
    elif equipos and not equipo and not nombre and not nombres:
        return read_consigna_multiple_by_equipo(db, equipos, start_date, end_date)
    else:
        raise HTTPException(status_code=400, detail="Debe proporcionar los datos de forma correcta.")


@router.get("/porcentaje")
def porcentaje_mode(db: Session = Depends(get_db), nombre=None, start_date=None, end_date=None):
    cache_key = f"porcentaje_{nombre}_{start_date}_{end_date}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    base_query = (
        select(
            func.count(ValoresConsigna.id_consigna).label('count'),
            ValoresConsigna.mode
        )
        .join(Consigna, ValoresConsigna.id_consigna == Consigna.id)
        .where(Consigna.nombre == nombre)
        .group_by(ValoresConsigna.mode)
    )

    if start_date:
        base_query = base_query.where(ValoresConsigna.timestamp >= start_date)
    if end_date:
        base_query = base_query.where(ValoresConsigna.timestamp <= end_date)

    resultados = db.execute(base_query).fetchall()

    total_count = sum(r.count for r in resultados)
    count_mode_1 = sum(r.count for r in resultados if r.mode == 1)
    count_mode_0 = sum(r.count for r in resultados if r.mode == 0)

    percentage_mode_1 = (count_mode_1 / total_count) * 100 if total_count > 0 else 0
    percentage_mode_0 = (count_mode_0 / total_count) * 100 if total_count > 0 else 0

    datos = {
        "consigna": nombre,
        "Automatico": f"{percentage_mode_1:.2f}%",
        "Manual": f"{percentage_mode_0:.2f}%"
    }

    set_cached_response(cache_key, datos)
    return datos


@router.get("/avg_modo")
def get_avg_modo(db: Session = Depends(get_db), nombre: str = None, start_date: str = None, end_date: str = None):
    cache_key = f"avg_modo_{nombre}_{start_date}_{end_date}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    base_query = (
        select(
            func.avg(ValoresConsigna.valor).label('avg'),
            Consigna.nombre.label('consigna'),
            ValoresConsigna.mode.label('mode')
        )
        .join(Consigna, ValoresConsigna.id_consigna == Consigna.id)
        .where(Consigna.nombre == nombre)
        .group_by(Consigna.nombre, ValoresConsigna.mode)
    )

    if start_date:
        base_query = base_query.where(ValoresConsigna.timestamp >= start_date)
    if end_date:
        base_query = base_query.where(ValoresConsigna.timestamp <= end_date)

    resultados = db.execute(base_query).fetchall()

    # Crear un diccionario para verificar modos presentes
    modos_presentes = {r.mode: r for r in resultados}

    # Prepara la respuesta asegurando que ambos modos estén presentes
    datos = []
    for mode in [0, 1]:  # 0 para MANUAL, 1 para AUTO
        if mode in modos_presentes:
            r = modos_presentes[mode]
            avg_value = r.avg
        else:
            avg_value = None

        mode_str = "AUTO" if mode == 1 else "MANUAL"
        datos.append({
            "avg": avg_value,
            "consigna": nombre,
            "mode": mode_str
        })

    set_cached_response(cache_key, datos)
    return datos
