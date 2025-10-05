Actividad 6.1 y 7.1 – OpenWeather (Berlín) - Flight dtata frfankfurt
Al02990943
Federico david Macias Orozco
Infraestructura para Big Data

En esta actividad generamos nuestra API key de OpenWeather, la guardamos de forma segura en la variable de entorno OPENWEATHER_API_KEY (sin subirla a GitHub) y desarrollamos un script en Python que consulta el endpoint de pronóstico 5 días / 3 horas para Berlín (/data/2.5/forecast) con units=metric y lang=es. El código obtiene el JSON, lo transforma a un DataFrame con columnas clave (time_utc, temperature, humidity, weather_status, wind_speed, rain_3h, snow_3h), normaliza tipos (fechas a datetime UTC y métricas numéricas), deduplica por time_utc conservando el registro más reciente y guarda los resultados en data/weather/berlin_forecast.csv. Finalmente verificamos la salida (mostrando las últimas filas) y publicamos el repositorio con el script y un .gitignore para no exponer credenciales ni datos crudos.


Actividad 7 Frankfurt
Para la actividad 7 recopilamos llegadas del aeropuerto de Fráncfort (FRA) usando AeroDataBox (RapidAPI). Consultamos el FIDS por rangos de 6 h (08:00–14:00 y 14:00–20:00 hora local) para cumplir el límite de la API (≤12 h), normalizamos el JSON con pandas.json_normalize, extraímos scheduled_arrival_utc y scheduled_arrival_frankfurt, junto con flight_number, aeropuerto de origen, aerolínea y modelo de aeronave; luego convertimos a datetime, filtramos filas sin hora y deduplicamos por (scheduled_arrival_utc, flight_number). Finalmente guardamos en data/flights/frankfurt_arrivals_tomorrow_divided.csv (UTF-8 con BOM para mostrar acentos correctamente en Excel).
