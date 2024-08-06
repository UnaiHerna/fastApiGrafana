from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import select, func, literal, extract, or_, and_
from sqlalchemy.orm import Session
from db.models import Variable, Equipo, Sensor, SensorDatos, SenalDatos, Senal, ValoresConsigna, Consigna
from db.connector import get_db
from routers import consigna, sensor, señal, sensorVacio
from utils.security import RateLimitMiddleware

app = FastAPI()

app.add_middleware(RateLimitMiddleware, max_requests_per_minute=100, max_requests_total=100, path_limit=100)

app.include_router(consigna.router)
app.include_router(sensor.router)
app.include_router(señal.router)
app.include_router(sensorVacio.router)

'''
# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://your-allowed-origin.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# OAuth2 configuration
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Include routers with dependencies
app.include_router(consigna.router, dependencies=[Depends(get_current_user)])
app.include_router(sensor.router, dependencies=[Depends(get_current_user)])
app.include_router(señal.router, dependencies=[Depends(get_current_user)])
'''

@app.get("/")
def read_root():
    return {"HolaMundo": "Bienvenido a mi API"}


@app.get("/variables/")
def read_variables(db: Session = Depends(get_db)):
    query = select(Variable).order_by(Variable.id)
    variables = db.execute(query).scalars().all()
    return variables


@app.get("/equipos/")
def read_equipos(db: Session = Depends(get_db)):
    equipos = db.execute(select(Equipo)).scalars().all()
    return equipos


@app.get("/relaciones/")
def read_relaciones(db: Session = Depends(get_db)):
    relaciones = db.execute(select(Sensor)).scalars().all()
    return relaciones


##############################################################################################################
# Consultas adicionales
##############################################################################################################


@app.get("/datos/solidos_suspendidos_totales_maxmin/")
def read_solidos_suspendidos_totales_max_min(db: Session = Depends(get_db)):
    try:
        # Subconsulta para obtener min y max valores por timestamp
        min_max_subquery = (
            select(
                SensorDatos.timestamp,
                func.min(SensorDatos.valor).label('min_valor'),
                func.max(SensorDatos.valor).label('max_valor')
            )
            .group_by(SensorDatos.timestamp)
            .subquery()
        )

        # Consulta principal uniendo con la subconsulta para obtener los valores originales
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


@app.get("/datos/promedio_valores_grandes_mes/")
def read_promedio_valores_grandes(db: Session = Depends(get_db)):
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
            .where(Variable.descripcion.in_(['Caudal de aire', 'Caudal de agua', 'Temperatura']))
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


@app.get("/datos/grafico2/")
def read_grafico2(db: Session = Depends(get_db)):
    try:
        subquery_senal = (
            select(
                SenalDatos.timestamp.label('time'),
                SenalDatos.valor.label('NNH4_FILT'),
            )
            .join(Senal, SenalDatos.id_señal == Senal.id)
            .where(Senal.nombre.__eq__('NNH4_FILT'))
            .subquery()
        )

        subquery_setpoint = (
            select(
                ValoresConsigna.timestamp.label('time'),
                ValoresConsigna.valor.label('DO_SP'),
            )
            .join(Consigna, ValoresConsigna.id_consigna == Consigna.id)
            .where(Consigna.nombre.__eq__('DO_SP'))
            .subquery()
        )

        nh4_query = (
            select(
                SensorDatos.timestamp.label('time'),
                SensorDatos.valor.label('NH4_Value'),
            )
            .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
            .join(Variable, Sensor.id_variable == Variable.id)
            .join(Equipo, Sensor.id_equipo == Equipo.id)
            .where(and_(Equipo.nombre == 'AER.COMB', Variable.simbolo == 'NH4'))
            .subquery()
        )

        do_query = (
            select(
                SensorDatos.timestamp.label('time'),
                SensorDatos.valor.label('DO_Value'),
            )
            .join(Sensor, (SensorDatos.id_equipo == Sensor.id_equipo) & (SensorDatos.id_variable == Sensor.id_variable))
            .join(Variable, Sensor.id_variable == Variable.id)
            .join(Equipo, Sensor.id_equipo == Equipo.id)
            .where(and_(Equipo.nombre == 'AER.DO', Variable.simbolo == 'DO'))
            .subquery()
        )

        query = (
            select(
                nh4_query.c.time,
                nh4_query.c.NH4_Value,
                do_query.c.DO_Value,
                subquery_senal.c.NNH4_FILT,
                subquery_setpoint.c.DO_SP,
            )
            .join(do_query, nh4_query.c.time == do_query.c.time)
            .join(subquery_senal, nh4_query.c.time == subquery_senal.c.time, isouter=True)
            .join(subquery_setpoint, nh4_query.c.time == subquery_setpoint.c.time, isouter=True)
            .order_by(nh4_query.c.time.asc())
        )

        resultados = db.execute(query).fetchall()
        datos = [
            {
                "time": r.time,
                "NH4_Value": r.NH4_Value,
                "DO_Value": r.DO_Value,
                "NNH4_FILT": r.NNH4_FILT,
                "DO_SP": r.DO_SP
            }
            for r in resultados
        ]
        return datos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al completar la query: {str(e)}")
