from typing import Optional
from fastapi import Depends, HTTPException, APIRouter, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from db.connector import get_db
from db.models import *
from db.redis_client import set_cached_response, get_cached_response
from datetime import datetime

router = APIRouter(
    prefix="/datos/consigna",
    tags=["consigna"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


async def read_datos_consigna_by_nombre(db: AsyncSession,
                                        consigna: str,
                                        start_date: Optional[datetime] = None,
                                        end_date: Optional[datetime] = None):
    cache_key = f"datos_consigna_{consigna}_{start_date}_{end_date}"
    cached_data = await get_cached_response(cache_key)
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
        .where(Consigna.nombre.__eq__(consigna))
        .order_by(ValoresConsigna.timestamp.asc())
    )

    if start_date:
        query = query.where(ValoresConsigna.timestamp >= start_date)
    if end_date:
        query = query.where(ValoresConsigna.timestamp <= end_date)

    resultados = (await db.execute(query)).fetchall()
    datos = [{"time": r.time, "value": r.value, "mode": r.mode, "consigna": r.consigna} for r in resultados]

    await set_cached_response(cache_key, datos)
    return datos


async def read_datos_consigna_by_equipo(db: AsyncSession,
                                        equipo: str,
                                        start_date: Optional[datetime] = None,
                                        end_date: Optional[datetime] = None):
    cache_key = f"datos_consigna_{equipo}_{start_date}_{end_date}"
    cached_data = await get_cached_response(cache_key)
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
        .where(Equipo.nombre.__eq__(equipo))
        .order_by(ValoresConsigna.timestamp.asc())
    )

    if start_date:
        query = query.where(ValoresConsigna.timestamp >= start_date)
    if end_date:
        query = query.where(ValoresConsigna.timestamp <= end_date)

    resultados = (await db.execute(query)).fetchall()
    datos = [{"time": r.time, "value": r.value, "mode": r.mode, "consigna": r.consigna} for r in resultados]

    await set_cached_response(cache_key, datos)
    return datos


async def read_consigna_multiple_by_nombre(db: AsyncSession,
                                           nombres: str,
                                           start_date: Optional[datetime] = None,
                                           end_date: Optional[datetime] = None):
    consigna_list = nombres.split(',')
    all_data = {}
    for consigna in consigna_list:
        data = await read_datos_consigna_by_nombre(db, consigna, start_date, end_date)
        all_data[consigna] = data
    return all_data


async def read_consigna_multiple_by_equipo(db: AsyncSession,
                                           equipos: str,
                                           start_date: Optional[datetime] = None,
                                           end_date: Optional[datetime] = None):
    equipo_list = equipos.split(',')
    all_data = {}
    for equipo in equipo_list:
        data = await read_datos_consigna_by_equipo(db, equipo, start_date, end_date)
        all_data[equipo] = data
    return all_data


@router.get("/")
async def datos_condicionales_consigna(
        nombre: Optional[str] = None,
        nombres: Optional[str] = None,
        equipo: Optional[str] = None,
        equipos: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        db: AsyncSession = Depends(get_db)
):
    if nombre and not equipo and not nombres and not equipos:
        return await read_datos_consigna_by_nombre(db, nombre, start_date, end_date)
    elif equipo and not nombre and not nombres and not equipos:
        return await read_datos_consigna_by_equipo(db, equipo, start_date, end_date)
    elif nombres and not equipo and not nombre and not equipos:
        return await read_consigna_multiple_by_nombre(db, nombres, start_date, end_date)
    elif equipos and not equipo and not nombre and not nombres:
        return await read_consigna_multiple_by_equipo(db, equipos, start_date, end_date)
    else:
        raise HTTPException(status_code=400, detail="Debe proporcionar los datos de forma correcta.")
