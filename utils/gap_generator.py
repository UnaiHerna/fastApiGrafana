import random


def generar_huecos(datos):
    huecos_totales = random.randint(1, 4)  # Número de huecos
    huecos_posiciones = set()
    huecos_info = []
    sigma = (len(datos) / 100) * 0.25
    # Generar huecos
    for _ in range(huecos_totales):
        long_hueco = int(
            min(len(datos) / 100 + sigma, max(len(datos) / 100 - sigma, int(random.gauss(len(datos) / 100, sigma)))))
        pos = random.randint(0, len(datos) - long_hueco)
        # Comprobar que no caigan 2 huecos en la misma posición
        if not any(pos + i in huecos_posiciones for i in range(long_hueco)):
            huecos_posiciones.update(range(pos, pos + long_hueco))
            huecos_info.append((pos, long_hueco, datos[pos]['time']))
    # Imprimir huecos
    for pos, length, time in huecos_info:
        print(f"Hueco en la posición: {pos} de {length} de largo, empezando en {time}")
    datos_with_gaps = [dato for i, dato in enumerate(datos) if i not in huecos_posiciones]
    return datos_with_gaps, huecos_info
