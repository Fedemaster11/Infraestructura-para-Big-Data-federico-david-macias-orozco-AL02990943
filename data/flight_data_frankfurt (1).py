import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from keys import flights_key, AERODATABOX_HOST # Clave y Host

# === BIBLIOTECAS AÑADIDAS PARA LA MIGRACIÓN A MySQL ===
from sqlalchemy import create_engine
import mysql.connector 
# ======================================================

# --- CONFIGURACIÓN DE ACCESO A MYSQL (¡ADAPTAR!) ---
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'Federico01'  # <<< ¡VERIFICAR QUE ESTA CONTRASEÑA SEA CORRECTA!
MYSQL_HOST = 'localhost'
MYSQL_DATABASE = 'gans'
# ----------------------------------------------------

# ----------------------------------------------------------------------
# 1) CONFIGURACIÓN Y RANGOS DE TIEMPO (BERLÍN)
# ----------------------------------------------------------------------

AIRPORT_CODE = "BER" # Código IATA de Berlín Brandeburgo
CODE_TYPE = "iata"
AERODATABOX_URL = f"https://{AERODATABOX_HOST}/flights/airports/{CODE_TYPE}/{AIRPORT_CODE}"
TIMEZONE = "Europe/Berlin"

# Calcula la fecha de mañana para la consulta
tomorrow = datetime.now() + timedelta(days=1)
day_str = tomorrow.strftime("%Y-%m-%d")

# Dividimos el rango total (08:00 a 20:00) en dos rangos de 6 horas
TIME_RANGES = [
    (f"{day_str}T08:00", f"{day_str}T14:00"), 
    (f"{day_str}T14:00", f"{day_str}T20:00"),
]

headers = {"x-rapidapi-host": AERODATABOX_HOST, "x-rapidapi-key": flights_key}
params = {
    "withLeg":"true", "direction":"Arrival", "withCancelled":"true",
    "withCodeshared":"true", "withCargo":"false", "withPrivate":"false", "withLocation":"false"
}

# ----------------------------------------------------------------------
# 2) FUNCIÓN DE EXTRACCIÓN Y LLAMADA A LA API
# ----------------------------------------------------------------------

def call_and_process_range(start_local: str, end_local: str) -> pd.DataFrame:
    """Realiza una llamada a la API para un rango y devuelve un DataFrame limpio."""
    url = f"{AERODATABOX_URL}/{start_local}/{end_local}"
    print(f"→ Obteniendo datos de {AIRPORT_CODE}: {start_local} a {end_local}...")

    # Petición a la API
    resp = requests.get(url, headers=headers, params=params, timeout=25)
    
    try:
        data = resp.json()
    except Exception:
        print(f"!! ERROR JSON. HTTP {resp.status_code}. Saltando este rango.")
        return pd.DataFrame()

    if resp.status_code != 200:
        print(f"!! ERROR HTTP {resp.status_code}: {data.get('message', 'Error API desconocido')}. Saltando este rango.")
        return pd.DataFrame()
    
    flights_data = data.get('arrivals', [])
    if not flights_data:
        print("-> No se encontraron vuelos en este rango.")
        return pd.DataFrame()

    #  Pedimos explícitamente 'utc' y 'local' 
    df = pd.json_normalize(
        flights_data,
        sep='.',
        meta=['number', ['airline', 'name'], ['aircraft', 'model'], 
              ['departure', 'airport', 'name'], 
              ['arrival', 'scheduledTime', 'utc'],
              ['arrival', 'scheduledTime', 'local']
              ],
        errors='ignore' 
    )

    # Renombrar columnas clave usando las rutas completas
    df = df.rename(columns={
        'arrival.scheduledTime.utc': 'scheduled_arrival_utc',
        'arrival.scheduledTime.local': 'scheduled_arrival_frankfurt',
        'number': 'flight_number',
        'departure.airport.name': 'from_airport_name',
        'airline.name': 'airline',
        'aircraft.model': 'aircraft_model'
    })

    # Seleccionamos solo las columnas relevantes
    COLUMNS_TO_CHECK = ['scheduled_arrival_utc', 'scheduled_arrival_frankfurt', 'flight_number', 'from_airport_name', 'airline', 'aircraft_model']
    df = df[[c for c in COLUMNS_TO_CHECK if c in df.columns]]
    
    # --------------------------------------------------------------------
    # LIMPIEZA DE TIEMPO
    # --------------------------------------------------------------------
    
    df["scheduled_arrival_utc"] = pd.to_datetime(df["scheduled_arrival_utc"], errors="coerce", utc=True)
    df["scheduled_arrival_frankfurt"] = pd.to_datetime(df["scheduled_arrival_frankfurt"], errors="coerce")
    df = df.dropna(subset=["scheduled_arrival_utc"])
    
    if df.empty:
        print("-> No se encontraron datos de tiempo válidos después de la limpieza.")

    return df

# ----------------------------------------------------------------------
# 3) EJECUCIÓN PRINCIPAL, COMBINACIÓN Y GUARDADO
# ----------------------------------------------------------------------

all_dfs = []
for start, end in TIME_RANGES:
    df_range = call_and_process_range(start, end)
    if not df_range.empty:
        all_dfs.append(df_range)

if not all_dfs:
    print("\nNo se pudieron obtener datos de vuelos. Terminando.")
    exit()

df = pd.concat(all_dfs, ignore_index=True)

# ----------------------------------------------------------------------
# 4) MIGRACIÓN A MYSQL Y DEDUPLICACIÓN (SOLUCIÓN FINAL AL ERROR)
# ----------------------------------------------------------------------

# 1. Mapear y adaptar el DataFrame (df) al esquema de la tabla flight_arrival

df['arrival_time'] = df['scheduled_arrival_utc']
df['airport_iata'] = AIRPORT_CODE # BER
df['airline_iata'] = df['airline'].str.split().str[0].str[:3] 
df['delay_minutes'] = None 

# >>>>>>>>>>>>> CORRECCIÓN CLAVE DEL ERROR 1054 <<<<<<<<<<<<<<
# La tabla SQL espera 'flight_icao' para el número de vuelo.
df_migracion = df.rename(columns={'flight_number': 'flight_icao'}) 
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# 2. Definir las columnas finales para insertar en el ORDEN CORRECTO
COLUMNAS_SQL_FINAL = [
    'flight_icao', 'arrival_time', 'airport_iata', 
    'airline_iata', 'delay_minutes'
]

# Asegurarse de que el DataFrame solo tenga las columnas que la tabla SQL acepta
df_migracion = df_migracion[[c for c in COLUMNAS_SQL_FINAL if c in df_migracion.columns]]


# 3. Conexión e Inserción a MySQL
try:
    print(f"\n→ Conectando a MySQL para migrar a {MYSQL_DATABASE}.flight_arrival...")
    engine = create_engine(
        f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
    )
    
    df_migracion.to_sql(
        name='flight_arrival',
        con=engine,
        if_exists='append',
        index=False
    )
    print(f" Migración exitosa. {len(df_migracion)} registros insertados en flight_arrival.")

except Exception as e:
    print(f" Error al conectar o insertar en MySQL: {e}")
    # Mostrar el error específico para el diagnóstico final si falla de nuevo
    print(f"DETALLES: {e}") 

# ----------------------------------------------------------------------
# MOSTRAR LAS PRIMERAS FILAS EN LA CONSOLA (Para verificación)
# ----------------------------------------------------------------------
print("\n--- Vista Previa de los Vuelos Recopilados ---\n")
print(df_migracion.head(10).to_string(index=False))