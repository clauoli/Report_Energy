import os
import psycopg2

DB_HOST = os.getenv("DB_HOST", "dpg-d32nlsjuibrs73a0u4ag-a")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "energy_ce")
DB_USER = os.getenv("DB_USER", "postgres2")
DB_PASS = os.getenv("DB_PASS", "oTLDleeGnCT8SOMQEgRNjlwZ8k60wgsl")

def get_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        print("Connessione al DB riuscita!")
        return conn
    except Exception as e:
        print("Errore nella connessione al DB:", e)
        return None
