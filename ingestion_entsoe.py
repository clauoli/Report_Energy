import pandas as pd
from entsoe import EntsoePandasClient
from datetime import datetime
import pytz
from connect_local import get_connection
import logging
from psycopg2.extras import execute_values
import os
import logging


# ------------------------------
# Logging con cartella /logs
# ------------------------------
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)  # crea la cartella se non esiste

timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")  # timestamp UTC

date_str = datetime.utcnow().strftime("%Y%m%d")
log_filename = os.path.join(logs_dir, f"import_{timestamp}.log")

logging.basicConfig(
    level=logging.INFO,
    filename=log_filename,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info(f"Inizio import dati")
# ------------------------------
# ENTSO-E API
# ------------------------------
API_KEY = os.getenv("ENTSOE_API_KEY", "default_api_key")
client = EntsoePandasClient(api_key=API_KEY)

# ------------------------------
# Intervallo di interesse
# ------------------------------
start = pd.Timestamp("2024-12-01T00:00Z")
end = pd.Timestamp("2025-01-31T23:00Z")

# ------------------------------
# Lista dei Paesi da caricare
# ------------------------------
countries = ["FR", "DE"]

# ------------------------------
# Helper per DB
# ------------------------------
def populate_countries(conn):
    countries_list = ["FR", "DE"]
    country_names = {"FR": "France", "DE": "Germany"}
    with conn.cursor() as cursor:
        for code in countries_list:
            name = country_names.get(code, code)
            try:
                cursor.execute("""
                    INSERT INTO countries(country_code, country_name)
                    VALUES (%s, %s)
                    ON CONFLICT (country_code) DO NOTHING;
                """, (code, name))
            except Exception as e:
                logging.error(f"Errore insert country {code}: {e}")
                conn.rollback()
    conn.commit()

def populate_energy_sources(conn, df):
    with conn.cursor() as cursor:
        for source in df.columns:
            # Se il nome della colonna è una tupla, prendi solo il primo elemento
            if isinstance(source, tuple):
                source_str = source[0]
            else:
                source_str = str(source)
            try:
                cursor.execute("""
                    INSERT INTO energy_sources(source_name)
                    VALUES (%s)
                    ON CONFLICT (source_name) DO NOTHING;
                """, (source_str,))
            except Exception as e:
                logging.error(f"Errore insert energy source {source_str}: {e}")
                conn.rollback()
    conn.commit()

def insert_production(conn, country_code, df):
    with conn.cursor() as cursor:
        for source_name in df.columns:
            # Prendi solo il primo elemento se è una tupla
            if isinstance(source_name, tuple):
                source_str = source_name[0]
            else:
                source_str = str(source_name)

            try:
                cursor.execute("SELECT source_id FROM energy_sources WHERE source_name=%s;", (source_str,))
                res = cursor.fetchone()
                if res:
                    source_id = res[0]
                else:
                    cursor.execute(
                        "INSERT INTO energy_sources(source_name) VALUES(%s) RETURNING source_id;",
                        (source_str,)
                    )
                    source_id = cursor.fetchone()[0]
            except Exception as e:
                logging.error(f"Errore query/insert source {source_str}: {e}")
                conn.rollback()
                continue

            # Lista dei valori da inserire nella tabella production
            values = []
            for ts, value in df[source_name].items():
                try:
                    ts_pd = pd.Timestamp(ts)
                    ts_utc = ts_pd.to_pydatetime().astimezone(pytz.UTC)
                    mwh = float(value)
                    values.append((country_code, source_id, ts_utc, mwh))
                except Exception as e:
                    logging.error(f"Errore preparazione production {country_code}, {source_str}, {ts}: {e}")

            if values:
                try:
                    execute_values(cursor, """
                        INSERT INTO production(country_code, source_id, timestamp, production_mwh)
                        VALUES %s
                        ON CONFLICT (country_code, source_id, timestamp) DO NOTHING;
                    """, values)
                except Exception as e:
                    logging.error(f"Errore batch insert production {country_code}, {source_str}: {e}")
                    conn.rollback()
    conn.commit()

def insert_consumption(conn, country_code, series):
    if isinstance(series, pd.DataFrame):
        series = series['Actual Load'] if 'Actual Load' in series.columns else series.iloc[:, 0]

    values = []
    for ts, value in series.items():
        try:
            ts_pd = pd.to_datetime(ts)
            ts_utc = ts_pd.to_pydatetime().astimezone(pytz.UTC)
            mwh = float(value)
            values.append((country_code, ts_utc, mwh))
        except Exception as e:
            logging.error(f"Errore preparazione consumption {country_code}, {ts}: {e}")

    if values:
        with conn.cursor() as cursor:
            try:
                execute_values(cursor, """
                    INSERT INTO consumption(country_code, timestamp, consumption_mwh)
                    VALUES %s
                    ON CONFLICT (country_code, timestamp) DO NOTHING;
                """, values)
            except Exception as e:
                logging.error(f"Errore batch insert consumption {country_code}: {e}")
                conn.rollback()
        conn.commit()

def insert_flows(conn, from_country, to_country, series):
    values = []
    for ts, value in series.items():
        try:
            ts_pd = pd.Timestamp(ts)
            ts_utc = ts_pd.to_pydatetime().astimezone(pytz.UTC)
            mwh = float(value)
            values.append((from_country, to_country, ts_utc, mwh))
        except Exception as e:
            logging.error(f"Errore preparazione flow {from_country}->{to_country}, {ts}: {e}")

    if values:
        with conn.cursor() as cursor:
            try:
                execute_values(cursor, """
                    INSERT INTO crossborder_flows(from_country, to_country, timestamp, flow_mwh)
                    VALUES %s
                    ON CONFLICT (from_country, to_country, timestamp) DO NOTHING;
                """, values)
            except Exception as e:
                logging.error(f"Errore batch insert flow {from_country}->{to_country}: {e}")
                conn.rollback()
        conn.commit()

# ------------------------------
# Main
# ------------------------------
def main():
    conn = get_connection()
    if not conn:
        logging.error(f"Impossibile connettersi al DB.")
        return

    populate_countries(conn)

    for country in countries:
        logging.info(f"Scaricando dati per {country}...")

        # Production
        try:
            prod_df = client.query_generation(country, start=start, end=end)
            if not prod_df.empty:
                populate_energy_sources(conn, prod_df)
                insert_production(conn, country, prod_df)
        except Exception as e:
            logging.error(f"Errore produzione {country}: {e}")
            conn.rollback()

        # Consumption
        try:
            cons_series = client.query_load(country, start=start, end=end)
            if not cons_series.empty:
                insert_consumption(conn, country, cons_series)
        except Exception as e:
            logging.error(f"Errore consumo {country}: {e}")
            conn.rollback()

    # Cross-border flows solo FR <-> DE
    country_pairs = [("FR", "DE"), ("DE", "FR")]
    for from_c, to_c in country_pairs:
        try:
            flow_series = client.query_crossborder_flows(from_c, to_c, start=start, end=end)
            if not flow_series.empty:
                insert_flows(conn, from_c, to_c, flow_series)
        except Exception as e:
            logging.error(f"Errore flussi {from_c}->{to_c}: {e}")
            conn.rollback()

    conn.close()
    logging.info(f"Import completato!")
if __name__ == "__main__":
    main()
