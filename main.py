from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import select, func, literal, extract, or_, and_
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware
from models.models import Variable, Equipo, Sensor, SensorDatos, SenalDatos, Senal, ValoresConsigna, Consigna
from schemas.schemas import VariableSchema, EquipoSchema, SensorSchema, SensorDatosSchema, SenalDatosSchema, SenalSchema, ValoresConsignaSchema, ConsignaSchema, DatosGrafico1Schema, DatosGrafico2Schema
from db.connector import get_db
from routers import consigna, sensor, señal, sensorVacio, forecast
from utils.security import RateLimitMiddleware

app = FastAPI()

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Cambia según sea necesario
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(RateLimitMiddleware, max_requests_per_minute=100, max_requests_total=100, path_limit=100)

app.include_router(consigna.router)
app.include_router(sensor.router)
app.include_router(señal.router)
app.include_router(sensorVacio.router)
app.include_router(forecast.router)

@app.get("/")
def read_root():
    return {"HolaMundo": "Bienvenido a mi API"}

@app.get("/variables/", response_model=list[VariableSchema])
def read_variables(db: Session = Depends(get_db)):
    query = select(Variable).order_by(Variable.id)
    variables = db.execute(query).scalars().all()
    return variables

@app.get("/equipos/", response_model=list[EquipoSchema])
def read_equipos(db: Session = Depends(get_db)):
    equipos = db.execute(select(Equipo)).scalars().all()
    return equipos

@app.get("/relaciones/", response_model=list[SensorSchema])
def read_relaciones(db: Session = Depends(get_db)):
    relaciones = db.execute(select(Sensor)).scalars().all()
    return relaciones

##############################################################################################################
# Consultas adicionales
##############################################################################################################

@app.get("/datos/solidos_suspendidos_totales_maxmin/")
def read_solidos_suspendidos_totales_max_min(db: Session = Depends(get_db)):
    try:
        min_max_subquery = (
            select(
                SensorDatos.timestamp,
                func.min(SensorDatos.valor).label('min_valor'),
                func.max(SensorDatos.valor).label('max_valor')
            )
            .group_by(SensorDatos.timestamp)
            .subquery()
        )

        query = (
            select(
                SensorDatos.timestamp,
                SensorDatos.valor,
                min_max_subquery.c.min_valor,
                min_max_subquery.c.max_valor
            )
            .join(min_max_subquery, min_max_subquery.c.timestamp == SensorDatos.timestamp)
        )

        resultados = db.execute(query).fetchall()
        datos = [
            {"timestamp": r.timestamp, "valor": r.valor, "min_valor": r.min_valor, "max_valor": r.max_valor}
            for r in resultados
        ]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la consulta: {str(e)}")

@app.get("/datos/promedio_valores_mes/")
def read_promedio_valores_mes(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                Variable.descripcion.label('metric'),
                func.avg(SensorDatos.valor).label('average_value'),
                func.concat(Equipo.nombre, literal(', ('), Variable.u_medida, literal(')')).label('equipo'),
                extract('year', SensorDatos.timestamp).label('year'),
                extract('month', SensorDatos.timestamp).label('month')
            )
            .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
            .join(Variable, Sensor.id_variable == Variable.id)
            .join(Equipo, Sensor.id_equipo == Equipo.id)
            .where(Variable.descripcion.in_(['Amonio', 'Nitrato', 'Oxígeno Disuelto', 'Sólidos Suspendidos Totales']))
            .group_by(Equipo.nombre, Variable.u_medida, Variable.descripcion, 'year', 'month')
        )
        resultados = db.execute(query).fetchall()
        datos = [
            {"metric": r.metric, "average_value": r.average_value, "equipo": r.equipo, "year": r.year, "month": r.month}
            for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")

@app.get("/datos/grafico1/")
def read_grafico1(db: Session = Depends(get_db)):
    try:
        query = (
            select(
                SensorDatos.timestamp.label('time'),
                SensorDatos.valor.label('value'),
                Variable.simbolo.label('variable')
            )
            .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
            .join(Variable, Sensor.id_variable == Variable.id)
            .join(Equipo, Sensor.id_equipo == Equipo.id)
            .where(or_(Equipo.nombre == 'AER.COMB', Equipo.nombre == 'AER.DO'))
            .order_by(SensorDatos.timestamp.asc())
        )
        resultados = db.execute(query).fetchall()
        datos = [{"time": r.time, "value": r.value, "variable": r.variable} for r in resultados]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")
