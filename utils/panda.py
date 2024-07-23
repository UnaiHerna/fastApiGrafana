import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Ruta al archivo CSV y detalles de la base de datos MySQL
csv_file1 = '../datos/Sensor_values.csv'
csv_file2 = '../datos/Setpoint_values.csv'
csv_file3 = '../datos/Filtered_values.csv'
mysql_username = 'root'
mysql_password = 'Cim12345!'
mysql_host = 'localhost'
mysql_dbname = 'datos'

# Crear una conexión a la base de datos MySQL
engine = create_engine(f'mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}/{mysql_dbname}')
Session = sessionmaker(bind=engine)
session = Session()

try:
    # SENSOR VALUES
    dtypes1 = {'timestamp': 'str', 'id_equipo': 'int', 'id_variable': 'int', 'valor': 'float'}
    df1 = pd.read_csv(csv_file1, skiprows=1, dtype=dtypes1, names=['timestamp', 'id_equipo', 'id_variable', 'valor'])
    df1.to_sql('sensor_datos', con=engine, if_exists='append', index=False)
    print(f'Datos importados exitosamente a la tabla "sensor_datos" en MySQL desde el archivo CSV.')

    # SETPOINT VALUES
    dtypes2 = {'timestamp': 'str', 'id_consigna': 'int', 'mode': 'int', 'valor': 'float'}
    df2 = pd.read_csv(csv_file2, skiprows=1, dtype=dtypes2, names=['timestamp', 'id_consigna', 'mode', 'valor'])
    df2.to_sql('valores_consigna', con=engine, if_exists='append', index=False)
    print(f'Datos importados exitosamente a la tabla "valores_consigna" en MySQL desde el archivo CSV.')

    # FILTERED VALUES
    dtypes3 = {'timestamp': 'str', 'id_señal': 'int', 'valor': 'float'}
    df3 = pd.read_csv(csv_file3, skiprows=1, dtype=dtypes3, names=['timestamp', 'id_señal', 'valor'])
    df3.to_sql('señal_datos', con=engine, if_exists='append', index=False)
    print(f'Datos importados exitosamente a la tabla "señal_datos" en MySQL desde el archivo CSV.')

    # Commit the transaction
    session.commit()
except SQLAlchemyError as e:
    # Rollback the transaction on error
    session.rollback()
    print(f'Error al importar datos: {str(e)}')
finally:
    # Close the session
    session.close()
    print("Sesión cerrada")
