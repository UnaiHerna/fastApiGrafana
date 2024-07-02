# main.py
from fastapi import FastAPI, HTTPException
from connector import connect_to_mysql, disconnect_from_mysql, get_mysql_cursor

app = FastAPI()


def convert_to_json(cursor):
    return [dict(row) for row in cursor]


# Ruta para obtener usuarios desde MySQL
@app.get("/usuarios/")
def obtener_usuarios():
    try:
        # Conectar a MySQL
        con = connect_to_mysql()

        # Ejemplo de consulta
        cursor = get_mysql_cursor(con)
        query = "SELECT * FROM usuario"
        cursor.execute(query)
        usuarios = cursor.fetchall()

        # Convertir resultado a JSON
        #usuarios = convert_to_json(cursor)

        # Desconectar de MySQL
        disconnect_from_mysql(con)

        return {"usuarios": usuarios}


    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener usuarios: {str(e)}")


@app.get("/samples10/")
def obtener_samples_altos():
    try:
        # Conectar a MySQL
        con = connect_to_mysql()

        # Obtener cursor de MySQL
        cursor = get_mysql_cursor(con)

        # Consulta SQL para obtener datos de muestras con valor mayor a 10
        query = """
            SELECT m.nombre AS nombre_maquina, s.nombre AS nombre_sample, s.valor 
            FROM maquina m 
            JOIN sample s ON s.id_maquina = m.id 
            WHERE s.valor > 10
        """
        cursor.execute(query)

        # Obtener todos los datos del cursor como una lista de diccionarios
        datos = cursor.fetchall()

        # Desconectar de MySQL
        disconnect_from_mysql(con)

        return {"datos": datos}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener datos: {str(e)}")

@app.post("/samples/")
def crear_sample():
    try:
        # Conectar a MySQL
        con = connect_to_mysql()

        # Obtener cursor de MySQL
        cursor = get_mysql_cursor(con)

        # Consulta SQL para insertar un nuevo sample
        query = "INSERT INTO sample (nombre, valor, id_maquina) VALUES (%s, %s, %s)"
        values = ("Sample 1", 15, 1)
        cursor.execute(query, values)

        # Confirmar cambios en la base de datos
        con.commit()

        # Desconectar de MySQL
        disconnect_from_mysql(con)

        return {"message": "Sample creado exitosamente"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear sample: {str(e)}")