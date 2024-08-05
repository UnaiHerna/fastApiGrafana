import datetime


def calcular_delta_prima(delta_dts, time_limits):
    diccionario1 = {
        "s": [1, 2, 3, 5, 10, 30],
        "m": [1, 2, 3, 5, 10, 30],
        "h": [1, 2, 3, 5, 12, 24, 48],
        "d": [1, 2]
    }

    diccionario2 = {
        's': 1,
        'm': 60,
        'h': 3600,
        'd': 86400
    }

    delta_dt = delta_dts * 1000
    max_data_points_visible_def = 549  # Ejemplo de máximo número de puntos visibles

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
     #print(values, num_time, b, agrupacion_rango_valor)
    z = agrupacion_rango_valor * diccionario2[values['unidad']]

    return z


def get_datos_sin_hueco(raw_data, raw_time, z):
    grouped_data = []
    current_group = []

    t0 = raw_time[0]
    intervalo = z * 1000  # milisegundos

    offset_ = 0

    t1 = t0 + intervalo

    for i in range(len(raw_data)):
        valor_offset = valor_offset_func(raw_data, raw_time, t0, t1, offset_)
        if valor_offset is not None:
            current_group.append(valor_offset['datos'])
            sum_ = sum(current_group[0])
            avg = sum_ / len(current_group[0])
            range_timestamps = raw_time[valor_offset['offset'][0]: valor_offset['offset'][1] + 1]
            average_timestamp = sum(range_timestamps) / len(range_timestamps)

            #formateamos fecha
            fecha_datetime = datetime.datetime.fromtimestamp(average_timestamp / 1000.0)
            grouped_data.append([fecha_datetime, round(avg, 2)])
            offset_ = valor_offset['offset'][1]
        else:
            average_timestamp = (t0 + t1) / 2
            missing_datetime = datetime.datetime.fromtimestamp(average_timestamp / 1000.0)
            grouped_data.append([missing_datetime, None])

            t0 = t1
            t1 = t0 + intervalo

        t0 = t1
        t1 = t0 + intervalo
        current_group = []
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
