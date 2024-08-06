import pandas as pd

# Lee el archivo CSV
df = pd.read_csv('../datos/Sensor_values.csv')

# Convierte la columna de fecha a datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Elimina las filas con fechas en 2024
df = df[df['timestamp'].dt.year != 2023]

# Actualiza las fechas en 2023 a 2024
df.loc[df['timestamp'].dt.year == 2024, 'timestamp'] = df['timestamp'].apply(
    lambda x: x.replace(year=2023)
)

# Guarda el DataFrame modificado en un nuevo archivo CSV
df.to_csv('../datos/Sensor_values.csv', index=False)

df = pd.read_csv('../datos/Setpoint_values.csv')
# Convierte la columna de fecha a datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Elimina las filas con fechas en 2024
df = df[df['timestamp'].dt.year != 2023]

# Actualiza las fechas en 2023 a 2024
df.loc[df['timestamp'].dt.year == 2024, 'timestamp'] = df['timestamp'].apply(
    lambda x: x.replace(year=2023)
)

# Guarda el DataFrame modificado en un nuevo archivo CSV
df.to_csv('../datos/Setpoint_values.csv', index=False)


df = pd.read_csv('../datos/Filtered_values.csv')
# Convierte la columna de fecha a datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Elimina las filas con fechas en 2024
df = df[df['timestamp'].dt.year != 2023]

# Actualiza las fechas en 2023 a 2024
df.loc[df['timestamp'].dt.year == 2024, 'timestamp'] = df['timestamp'].apply(
    lambda x: x.replace(year=2023)
)

# Guarda el DataFrame modificado en un nuevo archivo CSV
df.to_csv('../datos/Filtered_values.csv', index=False)
