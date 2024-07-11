import pandas as pd
from sqlalchemy import create_engine

# Ruta al archivo CSV y detalles de la base de datos MySQL
csv_file1 = 'datos/Sensor_values.csv'
csv_file2 = 'datos/Setpoint_values.csv'
csv_file3 = 'datos/Filtered_values.csv'
mysql_username = 'root'
mysql_password = 'Cim12345!'
mysql_host = 'localhost'
mysql_dbname = 'datos'

# Crear una conexión a la base de datos MySQL
engine = create_engine(f'mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}/{mysql_dbname}')


###SENSOR VALUES
# Leer el archivo CSV con pandas, omitiendo la primera fila (encabezados)
dtypes1 = {'timestamp': 'str', 'id_equipo': 'int', 'id_variable': 'int', 'valor': 'float'}
df1 = pd.read_csv(csv_file1, skiprows=1, dtype=dtypes1, names=['timestamp', 'id_equipo', 'id_variable', 'valor'])

# Insertar los datos en la tabla MySQL
try:
    df1.to_sql('sensor_datos', con=engine, if_exists='append', index=False)
    print(f'Datos importados exitosamente a la tabla "sensor_datos" en MySQL desde el archivo CSV.')
except Exception as e:
    print(f'Error al importar datos: {str(e)}')

###SETPOINT VALUES
dtypes2 = {'timestamp': 'str', 'id_consigna': 'int', 'mode': 'int', 'valor': 'float'}
df2 = pd.read_csv(csv_file2, skiprows=1, dtype=dtypes2, names=['timestamp', 'id_consigna', 'mode', 'valor'])

# Convertir los valores de 'mode' a booleanos
df2['mode'] = df2['mode'].astype(bool)

# Insertar los datos en la tabla MySQL
try:
    df2.to_sql('valores_consigna', con=engine, if_exists='append', index=False)
    print(f'Datos importados exitosamente a la tabla "valores_consigna" en MySQL desde el archivo CSV.')
except Exception as e:
    print(f'Error al importar datos: {str(e)}')

###FILTERED VALUES
dtypes3 = {'timestamp': 'str', 'id_señal': 'int', 'valor': 'float'}
df3 = pd.read_csv(csv_file3, skiprows=1, dtype=dtypes3, names=['timestamp', 'id_señal', 'valor'])

# Insertar los datos en la tabla MySQL
try:
    df3.to_sql('señal_datos', con=engine, if_exists='append', index=False)
    print(f'Datos importados exitosamente a la tabla "señal_datos" en MySQL desde el archivo CSV.')
except Exception as e:
    print(f'Error al importar datos: {str(e)}')
