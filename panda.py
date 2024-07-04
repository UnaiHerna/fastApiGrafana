import pandas as pd
from sqlalchemy import create_engine

# Ruta al archivo CSV y detalles de la base de datos MySQL
csv_file = 'Sensor_values.csv'
mysql_username = 'root'
mysql_password = 'Cim12345!'
mysql_host = 'localhost'
mysql_dbname = 'datos'

# Crear una conexión a la base de datos MySQL
engine = create_engine(f'mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}/{mysql_dbname}')

# Leer el archivo CSV con pandas, omitiendo la primera fila (encabezados)
dtypes = {'timestamp': 'str', 'id_equipo': 'int', 'id_variable': 'int', 'valor': 'float'}
df = pd.read_csv(csv_file, skiprows=1, dtype=dtypes, names=['timestamp', 'id_equipo', 'id_variable', 'valor'])

# Insertar los datos en la tabla MySQL
try:
    df.to_sql('datos', con=engine, if_exists='append', index=False)
    print(f'Datos importados exitosamente a la tabla "datos" en MySQL desde el archivo CSV.')
except Exception as e:
    print(f'Error al importar datos: {str(e)}')
