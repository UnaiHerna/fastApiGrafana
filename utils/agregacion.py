import datetime


def get_diccionario1(tipo):
    if tipo == "timeseries":
        diccionario1 = {
            "s": [1, 2, 3, 5, 10, 30],
            "m": [1, 2, 3, 5, 10, 30],
            "h": [1, 2, 3, 5, 12, 24, 48],
            "d": [1, 2]
        }
        max_points = 549
        return [diccionario1, max_points]
    elif tipo == "barchart":
        diccionario1 = {
            "h": [1, 2, 3, 5, 12, 24, 48],
            "d": [1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        }
        max_points = 10
        return [diccionario1, max_points]

    else:
        raise ValueError("Tipo de gráfico no válido")


def calcular_delta_prima(tipo, delta_dts, time_limits):

    diccionario1, max_data_points_visible_def = get_diccionario1(tipo)

    diccionario2 = {
        's': 1,
        'm': 60,
        'h': 3600,
        'd': 86400
    }

    delta_dt = delta_dts * 1000
    # max_data_points_visible_def = 549  # Ejemplo de máximo número de puntos visibles

    tiempo_inicial = int(time_limits[0].timestamp() * 1000)

    tiempo_final = int(time_limits[1].timestamp() * 1000)
    # Calcular la diferencia total de tiempo
    diferencia_total = tiempo_final - tiempo_inicial  # Diferencia en milisegundos
    # Calcular el total de intervalos
    l_totales = diferencia_total // delta_dt  # Total de intervalos de tiempo
    # l_totales = 345600  # Total de intervalos de tiempo

    # Calcular la variable a
    a = l_totales / max_data_points_visible_def

    b = a * delta_dts
    # print(a, b, l_totales)
    if b < delta_dts:
        return delta_dts
    # Determinar el rango de 'b' en Diccionario2
    k = -1
    claves_d1 = list(diccionario2.keys())

    for i in range(len(diccionario2)):
        key = claves_d1[i]
        if b < diccionario2[key]:
            break
        else:
            k += 1

    values = {
        "unidad": claves_d1[k],
        "valor": diccionario2[claves_d1[k]],
        "rango": diccionario1[claves_d1[k]]
    }
    num_time = b / values["valor"]

    agrupacion_rango_valor = 0

    for rango in values["rango"]:
        if num_time < rango:
            agrupacion_rango_valor = rango
            break
    z = agrupacion_rango_valor * diccionario2[values['unidad']]

    return z


def get_datos_sin_hueco(time_limits, raw_data, raw_time, z):
    grouped_data = []
    current_group = []

    t0 = int(time_limits[0].timestamp() * 1000)
    tiempo_final = int(time_limits[1].timestamp() * 1000)
    intervalo = z * 1000  # milisegundos

    offset_ = 0

    t1 = t0 + intervalo

    for i in range(len(raw_data)):
        valor_offset = valor_offset_func(raw_data, raw_time, t0, t1, offset_)
        if valor_offset is not None:
            current_group.append(valor_offset['datos'])
            sum_ = sum(current_group[0])
            avg = sum_ / len(current_group[0])
            #formateamos fecha
            grouped_data.append([datetime.datetime.fromtimestamp(t0 / 1000.0), round(avg, 2)])
            offset_ = valor_offset['offset'][1]
        else:
            grouped_data.append([datetime.datetime.fromtimestamp(t0 / 1000.0), None])

        t0 = t1
        t1 = t0 + intervalo
        current_group = []

        if t0 > tiempo_final:
            break

    return grouped_data


def valor_offset_func(row_data, row_time, t0, t1, pos_in):
    indices = []
    datos = []

    if row_time[0] > t1:
        return None

    if row_time[-1] < t0:
        return None

    for i in range(pos_in, len(row_time)):
        if t0 <= row_time[i] <= t1:
            datos.append(row_data[i])
            indices.append(i)
        elif row_time[i] > t1:
            break

    if not indices:
        return None

    current_data = {
        'datos': datos,
        'offset': [indices[0], indices[-1]],
    }

    return current_data


'''
def convertir_a_zona_horaria_sin_dst(dt, zona_horaria_str):
    utc = pytz.utc
    zona_horaria = pytz.timezone(zona_horaria_str)

    # Convertir a UTC
    dt_utc = dt.replace(tzinfo=utc)

    # Convertir a la zona horaria deseada sin ajuste DST
    dt_sin_dst = dt_utc.astimezone(zona_horaria)
    return dt_sin_dst.replace(tzinfo=None)


def alinear_a_intervalo(dt, intervalo_horas):
    # Redondear la fecha y hora al intervalo más cercano (00:00 o 12:00)
    if dt.hour < 12:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        return dt.replace(hour=12, minute=0, second=0, microsecond=0)


def get_datos_sin_hueco(time_limits, raw_data, raw_time, z):
    grouped_data = []
    current_group = []

    t0 = int(time_limits[0].timestamp() * 1000)
    tiempo_final = int(time_limits[1].timestamp() * 1000)
    intervalo = z * 1000  # milisegundos

    offset_ = 0

    t1 = t0 + intervalo

    for i in range(len(raw_data)):
        valor_offset = valor_offset_func(raw_data, raw_time, t0, t1, offset_)
        if valor_offset is not None:
            current_group.append(valor_offset['datos'])
            sum_ = sum(current_group[0])
            avg = sum_ / len(current_group[0])
            # Formateamos fecha y la convertimos a la zona horaria deseada
            dt = datetime.fromtimestamp(t0 / 1000.0)
            dt_ajustado = alinear_a_intervalo(dt, 12)  # Alinear a 00:00 o 12:00
            dt_sin_dst = convertir_a_zona_horaria_sin_dst(dt_ajustado, 'Europe/Madrid')  # Por ejemplo, 'Europe/Madrid'
            grouped_data.append([dt_sin_dst, round(avg, 2)])
            offset_ = valor_offset['offset'][1]
        else:
            dt = datetime.fromtimestamp(t0 / 1000.0)
            dt_ajustado = alinear_a_intervalo(dt, 12)  # Alinear a 00:00 o 12:00
            dt_sin_dst = convertir_a_zona_horaria_sin_dst(dt_ajustado, 'Europe/Madrid')  # Por ejemplo, 'Europe/Madrid'
            grouped_data.append([dt_sin_dst, None])

        t0 = t1
        t1 = t0 + intervalo
        current_group = []

        if t0 > tiempo_final:
            break

    return grouped_data
'''
