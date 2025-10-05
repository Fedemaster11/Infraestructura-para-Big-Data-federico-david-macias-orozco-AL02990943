import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from keys import flights_key, AERODATABOX_HOST # Clave y Host

# ----------------------------------------------------------------------
# 1) CONFIGURACIÓN Y RANGOS DE TIEMPO (FRANKFURT)
# ----------------------------------------------------------------------

AIRPORT_CODE = "FRA" # Código IATA de Fráncfort
CODE_TYPE = "iata"
AERODATABOX_URL = f"https://{AERODATABOX_HOST}/flights/airports/{CODE_TYPE}/{AIRPORT_CODE}"
TIMEZONE = "Europe/Berlin"

# Calcula la fecha de mañana para la consulta
tomorrow = datetime.now() + timedelta(days=1)
day_str = tomorrow.strftime("%Y-%m-%d")

# Dividimos el rango total (08:00 a 20:00) en dos rangos de 6 horas
TIME_RANGES = [
    (f"{day_str}T08:00", f"{day_str}T14:00"), # Primer rango de 6 horas
    (f"{day_str}T14:00", f"{day_str}T20:00"), # Segundo rango de 6 horas
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
              ['arrival', 'scheduledTime', 'utc'],   # <-- Pedido de la clave 'utc'
              ['arrival', 'scheduledTime', 'local'] # <-- Pedido de la clave 'local'
              ],
        errors='ignore' 
    )

    # Renombrar columnas clave usando las rutas completas
    df = df.rename(columns={
        'arrival.scheduledTime.utc': 'scheduled_arrival_utc',       # <-- Columna final UTC
        'arrival.scheduledTime.local': 'scheduled_arrival_frankfurt', # <-- Columna final local
        'number': 'flight_number',
        'departure.airport.name': 'from_airport_name',
        'airline.name': 'airline',
        'aircraft.model': 'aircraft_model'
    })

    # Seleccionamos solo las columnas relevantes
    COLUMNS_TO_CHECK = ['scheduled_arrival_utc', 'scheduled_arrival_frankfurt', 'flight_number', 'from_airport_name', 'airline', 'aircraft_model']
    df = df[[c for c in COLUMNS_TO_CHECK if c in df.columns]]
    
    # --------------------------------------------------------------------
    # LIMPIEZA DE TIEMPO (USANDO LOS DATOS OBTENIDOS DIRECTAMENTE)
    # --------------------------------------------------------------------
    
    # Intentamos convertir las columnas. Si no existen, Pandas simplemente les da NaT.
    df["scheduled_arrival_utc"] = pd.to_datetime(df["scheduled_arrival_utc"], errors="coerce", utc=True)
    
    # La columna local ya viene con la zona horaria en el string de la API, solo la convertimos
    df["scheduled_arrival_frankfurt"] = pd.to_datetime(df["scheduled_arrival_frankfurt"], errors="coerce")

    # Eliminamos filas donde no hay tiempo UTC (vuelos incompletos)
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
# 4) DEDUPLICACIÓN Y GUARDADO FINAL
# ----------------------------------------------------------------------

DEDUP_SUBSET = ["scheduled_arrival_utc", "flight_number"]
final_cols = [
    "scheduled_arrival_utc", "scheduled_arrival_frankfurt", "flight_number",
    "from_airport_name", "airline", "aircraft_model"
]

out_dir = Path("data/flights")
out_dir.mkdir(parents=True, exist_ok=True)
out_csv = out_dir / "frankfurt_arrivals_tomorrow_divided.csv" 

# Lógica de deduplicación con archivos antiguos
if out_csv.exists():
    old = pd.read_csv(out_csv, parse_dates=["scheduled_arrival_utc", "scheduled_arrival_frankfurt"])
    df = pd.concat([old, df], ignore_index=True)

df = df.sort_values("scheduled_arrival_utc").drop_duplicates(
    subset=DEDUP_SUBSET, keep="last"
)

df = df[[c for c in final_cols if c in df.columns]]
df.to_csv(out_csv, index=False, encoding="utf-8-sig") # Guardado robusto


print(f"\nDatos guardados y deduplicados en: {out_csv.resolve()}")
print(f"Total de registros finales: {len(df)}")

# ----------------------------------------------------------------------
# MOSTRAR LAS PRIMERAS FILAS EN LA CONSOLA
# ----------------------------------------------------------------------

display_cols = [
    "flight_number", 
    "from_airport_name", 
    "airline", 
    "aircraft_model",
    "scheduled_arrival_utc", 
    "scheduled_arrival_frankfurt" 
]

df_display = df[[c for c in display_cols if c in df.columns]]

print("\n--- Vista Previa de los Vuelos Recopilados ---\n")
print(df_display.head(10).to_string(index=False))