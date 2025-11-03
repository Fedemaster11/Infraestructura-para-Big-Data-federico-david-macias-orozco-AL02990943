import os
import requests
import pandas as pd
from pathlib import Path

# === BIBLIOTECAS AÑADIDAS PARA LA MIGRACIÓN A MySQL ===
from sqlalchemy import create_engine
import mysql.connector 
# ======================================================

# --- CONFIGURACIÓN DE ACCESO A MYSQL (¡ADAPTAR!) ---
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'Federico01'  # <<< ¡REEMPLAZAR CON TU NUEVA CONTRASEÑA!
MYSQL_HOST = 'localhost'
MYSQL_DATABASE = 'gans'
# ----------------------------------------------------

# 1) API key desde variable de entorno (limpia espacios/nuevas líneas)
API_KEY = (os.getenv("OPENWEATHER_API_KEY") or "").strip()
if not API_KEY:
    raise RuntimeError("No se encontró la API key. Define OPENWEATHER_API_KEY en tu shell.")

# 2) Ciudad y parámetros
CITY = "Berlin"
COUNTRY = "DE"
URL = "https://api.openweathermap.org/data/2.5/forecast"
params = {
    "q": f"{CITY},{COUNTRY}",
    "appid": API_KEY,
    "units": "metric",
    "lang": "es"
}

# 3) Petición
resp = requests.get(URL, params=params, timeout=20)
try:
    data = resp.json()
except Exception:
    raise RuntimeError(f"No se pudo decodificar JSON. HTTP {resp.status_code}: {resp.text[:200]}")

if resp.status_code != 200:
    raise RuntimeError(f"Error {resp.status_code}: {data}")

# 4) Parseo de datos → DataFrame (df_weather)
rows = []
for entry in data.get("list", []):
    rows.append({
        "time_utc": entry.get("dt_txt"),
        "temperature": (entry.get("main") or {}).get("temp"),
        "humidity": (entry.get("main") or {}).get("humidity"),
        "weather_status": (entry.get("weather") or [{}])[0].get("main"),
        "wind_speed": (entry.get("wind") or {}).get("speed"),
        "rain_3h": (entry.get("rain") or {}).get("3h"),
        "snow_3h": (entry.get("snow") or {}).get("3h")
    })

df_weather = pd.DataFrame(rows)

# 4.1) Limpieza de tipos y conversión a UTC
df_weather["time_utc"] = pd.to_datetime(df_weather["time_utc"], errors="coerce", utc=True)
for c in ["temperature", "humidity", "wind_speed", "rain_3h", "snow_3h"]:
    if c in df_weather.columns:
        df_weather[c] = pd.to_numeric(df_weather[c], errors="coerce")

# Limpieza adicional (ya no es clave para la deduplicación en CSV, pero es buena práctica)
df_weather = df_weather.sort_values("time_utc").drop_duplicates(subset=["time_utc"], keep="last")

# ----------------------------------------------------------------------
# 5) MIGRACIÓN DIRECTA A MYSQL (REEMPLAZANDO LA LÓGICA DE GUARDADO EN CSV)
# ----------------------------------------------------------------------

# 1. Adaptar el DataFrame (df_weather) al esquema de la tabla weather_data
df_weather['timestamp'] = df_weather['time_utc']
df_weather['weather_description'] = df_weather['weather_status'] # Mapeo de status a description
df_weather['city'] = CITY # Añadir el nombre de la ciudad

# 2. Definir las columnas finales para insertar en la tabla weather_data
COLUMNAS_MYSQL = [
    'timestamp', 'temperature', 'humidity', 'wind_speed', 
    'weather_description', 'city'
]

df_migracion = df_weather[[c for c in COLUMNAS_MYSQL if c in df_weather.columns]]

# 3. Conexión e Inserción a MySQL
try:
    print(f"\n→ Conectando a MySQL para migrar a {MYSQL_DATABASE}.weather_data...")
    # Usamos el usuario, contraseña y base de datos definidos al inicio
    engine = create_engine(
        f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
    )
    
    df_migracion.to_sql(
        name='weather_data',
        con=engine,
        if_exists='append', # Añade los nuevos registros a la tabla existente
        index=False
    )
    print(f" Migración exitosa. {len(df_migracion)} registros insertados en weather_data.")

except Exception as e:
    print(f" Error al conectar o insertar en MySQL: {e}")
    # En caso de error de conexión, el script no debe detenerse si estás en un Jupyter Notebook.

# ----------------------------------------------------------------------
# MOSTRAR LAS PRIMERAS FILAS EN LA CONSOLA (Para verificación)
# ----------------------------------------------------------------------
print("\n--- Vista Previa de los Datos Migrados ---\n")
print(df_migracion.head(5).to_string(index=False))