import os
import requests
import pandas as pd
from pathlib import Path

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

# 4) Parseo de datos → DataFrame
rows = []
for entry in data.get("list", []):
    rows.append({
        "time_utc": entry.get("dt_txt"),  # string UTC
        "temperature": (entry.get("main") or {}).get("temp"),
        "humidity": (entry.get("main") or {}).get("humidity"),
        "weather_status": (entry.get("weather") or [{}])[0].get("main"),
        "wind_speed": (entry.get("wind") or {}).get("speed"),
        "rain_3h": (entry.get("rain") or {}).get("3h"),
        "snow_3h": (entry.get("snow") or {}).get("3h")
    })

df = pd.DataFrame(rows)

# 4.1) Limpieza de tipos (clave para deduplicar bien)
df["time_utc"] = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
for c in ["temperature", "humidity", "wind_speed", "rain_3h", "snow_3h"]:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

#  Hora local de Berlín
try:
    df["time_berlin"] = df["time_utc"].dt.tz_convert("Europe/Berlin")
except Exception:
    df["time_berlin"] = df["time_utc"]

# 5) Guardar en CSV (en carpeta data/weather) con deduplicado por time_utc
out_dir = Path("data/weather")
out_dir.mkdir(parents=True, exist_ok=True)
out_csv = out_dir / "berlin_forecast.csv"

if out_csv.exists():
    old = pd.read_csv(out_csv)
    # Normaliza tipos por si versiones anteriores guardaron como texto
    if "time_utc" in old.columns:
        old["time_utc"] = pd.to_datetime(old["time_utc"], errors="coerce", utc=True)
    for c in ["temperature", "humidity", "wind_speed", "rain_3h", "snow_3h"]:
        if c in old.columns:
            old[c] = pd.to_numeric(old[c], errors="coerce")
    if "time_berlin" in old.columns:
        # No es imprescindible para dedup, pero lo mantenemos si existe
        old["time_berlin"] = pd.to_datetime(old["time_berlin"], errors="coerce", utc=True)

    df = pd.concat([old, df], ignore_index=True)

# Ordena por tiempo y quita duplicados (conserva el registro más reciente por instante)
df = df.sort_values("time_utc").drop_duplicates(subset=["time_utc"], keep="last")

# Reordena columnas de salida (incluye time_berlin si existiera)
cols = ["time_utc", "time_berlin", "temperature", "humidity", "weather_status", "wind_speed", "rain_3h", "snow_3h"]
df = df[[c for c in c]()]()
