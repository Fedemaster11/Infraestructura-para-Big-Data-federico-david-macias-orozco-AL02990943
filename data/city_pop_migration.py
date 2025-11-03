import pandas as pd
from sqlalchemy import create_engine
import mysql.connector

# --- CONFIGURACIÓN DE ACCESO A MYSQL ---
USER = 'root'
PASSWORD = 'Federico01'  # Tu contraseña de root
HOST = 'localhost'
DATABASE = 'gans'
# ----------------------------------------

# Define la ciudad de tu caso de estudio
CITY_NAME = "Berlin" 
COUNTRY_CODE = "DE" 

try:
    # 1. Cargar datos usando la RUTA COMPLETA (¡CORREGIDO!)
    CSV_PATH = 'C:/Users/feder/Dropbox/PC/Downloads/worldcities.csv'
    df_cities = pd.read_csv(CSV_PATH)

    # 2. Filtrar y preparar la columna
    df_filtered = df_cities[
        (df_cities['city_ascii'] == CITY_NAME) & 
        (df_cities['iso2'] == COUNTRY_CODE)
    ].copy()

    # Crea la columna municipality_iso_country: "Berlin,DE"
    df_filtered['municipality_iso_country'] = df_filtered.apply(
        lambda row: f"{row['city_ascii']},{row['iso2']}", axis=1
    )

    # Seleccionar columnas finales
    df_final = df_filtered[[
        'city_ascii', 'lat', 'lng', 'population', 'municipality_iso_country'
    ]].rename(columns={'city_ascii': 'city'})


    # 3. Conexión e Inserción a MySQL
    engine = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DATABASE}")
    
    df_final.to_sql(
        name='city_pop',
        con=engine,
        if_exists='append',
        index=False
    )
    print(f"✅ Datos de {CITY_NAME} insertados correctamente en la tabla city_pop.")

except Exception as e:
    print(f"❌ ERROR: Fallo al leer el archivo o conectar a MySQL: {e}")