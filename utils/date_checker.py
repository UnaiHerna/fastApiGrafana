from fastapi import HTTPException
from datetime import datetime
from db.models import SensorDatos


def date_validator(query, end_date, start_date):
    # Obtener la fecha y hora actuales
    current_datetime = datetime.now()

    if start_date and start_date >= datetime(2024, 1, 1):
        query = query.where(SensorDatos.timestamp >= start_date)
    else:
        raise HTTPException(status_code=400, detail="La fecha de inicio debe ser posterior a 2023")
    if end_date and end_date > start_date:
        query = query.where(SensorDatos.timestamp <= end_date)
    else:
        raise HTTPException(status_code=400, detail="La fecha de inicio debe ser menor a la fecha de fin.")
    # Validar que end_date no sea posterior a la fecha actual
    query = query.where(SensorDatos.timestamp <= current_datetime)
    return query
