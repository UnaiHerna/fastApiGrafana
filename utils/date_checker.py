from fastapi import HTTPException
from db.models import ValoresConsigna


def date_checker(start_date, end_date):
    if start_date and end_date:
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="La fecha de inicio no puede ser mayor a la fecha de fin")