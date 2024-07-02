import mysql.connector

# Configuración de conexión a MySQL
def connect_to_mysql():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Cim12345",
        database="depuradoradb"
    )

def disconnect_from_mysql(cnx):
    cnx.close()

# Configuración para obtener un cursor
def get_mysql_cursor(cnx):
    return cnx.cursor(dictionary=True)
