import pandas as pd
from sqlalchemy import create_engine
from time import time

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
df_iter = pd.read_csv('nyt.csv', iterator=True, chunksize=100000)
df = pd.read_csv('nyt.csv')
df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

try:
    while True: 
        t_start = time()  # Captura el tiempo de inicio del bucle

        try:
            df = next(df_iter)  # Obtiene el siguiente chunk de datos del iterador df_iter
        except StopIteration:
            print("No more data to process.")
            break

        # Convierte las columnas de fechas a tipos datetime si es necesario
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        try:
            # Inserta el chunk actual del DataFrame en la tabla 'yellow_taxi_data' en la base de datos usando SQLAlchemy
            df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
        except Exception as e:
            print(f"Error inserting chunk into database: {e}")
            continue

        t_end = time()  # Captura el tiempo al finalizar la inserción del chunk

        # Imprime el tiempo que tomó insertar el chunk actual
        print(f"Inserted another chunk, took {t_end - t_start:.3f} seconds")

except Exception as e:
    print(f"An error occurred during processing: {e}")