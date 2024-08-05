from locust import HttpUser, task, between

class APIUser(HttpUser):
    wait_time = between(1, 5)  # Tiempo de espera entre peticiones, en segundos

    @task(1)  # La proporción de esta tarea en relación con otras
    def get_root(self):
        self.client.get("/")

    @task(2)
    def get_variables(self):
        self.client.get("/variables/")

    @task(2)
    def get_equipos(self):
        self.client.get("/equipos/")

    @task(3)
    def get_datos_solidos_suspendidos(self):
        self.client.get("/datos/solidos_suspendidos_totales_maxmin/")

    @task(3)
    def get_datos_promedio_valores_mes(self):
        self.client.get("/datos/promedio_valores_mes/")

    @task(3)
    def get_datos_promedio_valores_grandes(self):
        self.client.get("/datos/promedio_valores_grandes_mes/")

    @task(3)
    def get_datos_grafico1(self):
        self.client.get("/datos/grafico1/")

    @task(3)
    def get_datos_grafico2(self):
        self.client.get("/datos/grafico2/")

    def prueba(self):
        self.client.get("/datos/sensorvacio/?variable=temp&equipo=INF_PIPE.CNTL&start_date=2023-01-01&end_date=2023"
                        "-04-02")
        self.client.get("/datos/sensorvacio/?variable=temp&equipo=INF_PIPE.CNTL&start_date=2023-01-01&end_date=2024"
                        "-01-01")
