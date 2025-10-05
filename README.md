Actividad 6.1 – OpenWeather (Berlín)
Al02990943
Federico david Macias Orozco
Infraestructura para Big Data

En esta actividad generamos nuestra API key de OpenWeather, la guardamos de forma segura en la variable de entorno OPENWEATHER_API_KEY (sin subirla a GitHub) y desarrollamos un script en Python que consulta el endpoint de pronóstico 5 días / 3 horas para Berlín (/data/2.5/forecast) con units=metric y lang=es. El código obtiene el JSON, lo transforma a un DataFrame con columnas clave (time_utc, temperature, humidity, weather_status, wind_speed, rain_3h, snow_3h), normaliza tipos (fechas a datetime UTC y métricas numéricas), deduplica por time_utc conservando el registro más reciente y guarda los resultados en data/weather/berlin_forecast.csv. Finalmente verificamos la salida (mostrando las últimas filas) y publicamos el repositorio con el script y un .gitignore para no exponer credenciales ni datos crudos.
