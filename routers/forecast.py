import os
import requests
from fastapi import Depends, HTTPException, APIRouter, status
from db.redis_client import set_cached_response, get_cached_response
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

router = APIRouter(
    prefix="/forecast",
    tags=["forecast"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)

# Cargar las variables de entorno desde .env
load_dotenv()

AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"
API_KEY = os.getenv("AEMET_API_KEY")

def formatear_hora(hora):
    if hora == 0:
        return "12am"
    elif hora < 12:
        return f"{hora}am"
    elif hora == 12:
        return "12pm"
    else:
        return f"{hora - 12}pm"


def obtener_datos_proximas_6_horas(datos):
    ahora = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=1))).hour
    proximas6Horas = []

    hoy = datos[0]  # Día actual
    for i in range(6):
        hora = (ahora + i) % 24
        periodo = f"{hora:02d}"
        tiempo = "ahora" if i == 0 else formatear_hora(hora)

        datos_hora = next((item for item in hoy["estadoCielo"] if item["periodo"] == periodo), None)
        temperatura = next((temp["value"] for temp in hoy["temperatura"] if temp["periodo"] == periodo),
                           "No disponible")
        probabilidad_precipitacion = next(
            (prec["value"] for prec in hoy["probPrecipitacion"] if prec["periodo"] == periodo), 0)

        if datos_hora:
            proximas6Horas.append({
                "tiempo": tiempo,
                "icono": datos_hora["descripcion"],
                "temperatura": temperatura,
                "probabilidadPrecipitacion": probabilidad_precipitacion
            })
    return proximas6Horas

def obtener_datos(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Error al obtener los datos")
    return response.json()

@router.get("/diario/{municipio_id}")
def obtener_prediccion_diaria(municipio_id: str):
    ahora = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=1))).hour
    cache_key = f"forecastDiario_{municipio_id}_{ahora}"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    if not API_KEY:
        raise HTTPException(status_code=500, detail="Falta la clave de API de AEMET")

    data = obtener_datos(
        f"{AEMET_BASE_URL}/prediccion/especifica/municipio/diaria/{municipio_id}/?api_key={API_KEY}")

    forecast_data = obtener_datos(data["datos"])

    forecast = [
        {
            "fecha": day["fecha"],
            "iconoCielo": day["estadoCielo"][0].get("descripcion", "Desconocido"),
            "temperaturaMaxima": day["temperatura"].get("maxima", "No disponible"),
            "temperaturaMinima": day["temperatura"].get("minima", "No disponible"),
            "probabilidadPrecipitacion": day["probPrecipitacion"][0].get("value", 0),
        }
        for day in forecast_data[0]["prediccion"]["dia"]
    ]

    set_cached_response(cache_key, forecast)
    return forecast


@router.get("/horario/{municipio_id}")
def obtener_prediccion_horaria(municipio_id: str):
    ahora = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=1))).hour
    cache_key = f"forecastHorario_{municipio_id}_{ahora}"
    cached_data = get_cached_response(cache_key)

    if not API_KEY:
        raise HTTPException(status_code=500, detail="Falta la clave de API de AEMET")

    try:
        data = obtener_datos(
            f"{AEMET_BASE_URL}/prediccion/especifica/municipio/horaria/{municipio_id}/?api_key={API_KEY}")
        forecast_data = obtener_datos(data["datos"])
        datos_finales = obtener_datos_proximas_6_horas(forecast_data[0]["prediccion"]["dia"])

        set_cached_response(cache_key, datos_finales)
        return datos_finales

    except Exception as e:
        if cached_data:
            return cached_data
        raise HTTPException(status_code=500, detail=f"Error obteniendo el clima: {str(e)}")


@router.get("/municipios")
def obtener_lista_municipios():
    cache_key = "municipios_lista"
    cached_data = get_cached_response(cache_key)
    if cached_data:
        return cached_data

    if not API_KEY:
        raise HTTPException(status_code=500, detail="Falta la clave de API de AEMET")

    data = obtener_datos(f"{AEMET_BASE_URL}/maestro/municipios/?api_key={API_KEY}")
    municipios = obtener_datos(data["datos"])

    set_cached_response(cache_key, municipios)
    return municipios
