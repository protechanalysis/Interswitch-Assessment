import logging
import sqlite3
from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import ClickHouseError
from airflow.hooks.base import BaseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

con_id = "clickhouse_default"
query_clickhouse = "/opt/airflow/dags/sql_Script/task1.sql"

## with connection management
def get_clickhouse_client(conn_id=con_id):
    """
    Establish and return a ClickHouse client connection using Airflow's connection metadata.

    This function retrieves connection details stored in Airflow via the provided 
    connection ID, then initializes a ClickHouse client with those parameters. 
    It logs both the connection attempt and its success, and raises an exception 
    if the connection fails.

    Args:
        conn_id (str): The Airflow connection ID for ClickHouse. Defaults to `con_id`.

    Returns:
        clickhouse_connect.driver.Client: An authenticated ClickHouse client instance.

    Raises:
        ClickHouseError: If the connection to ClickHouse cannot be established.
    """
    try:
        logging.info("Establishing connection to ClickHouse")
        hook = BaseHook.get_connection(conn_id)
        client = get_client(
            host=hook.host,
            port=hook.port,
            username=hook.login,
            password=hook.password,
            database=hook.schema,
        )
        logging.info("Connection to ClickHouse established successfully")
        return client
        
    except ClickHouseError as ce:
        logging.error(f"Error connecting to ClickHouse: {ce}")
        raise

def query_clickhouse(query_file: str = query_clickhouse):
    """
    Execute a SQL query against ClickHouse using a query file.

    This function reads a SQL statement from the provided file,
    executes it on ClickHouse via a managed connection, and returns the results.

    Args:
        query_file (str): Path to the SQL file containing the query. 
                          Defaults to `query_clickhouse`.

    Returns:
        list[tuple]: A list of result rows returned by ClickHouse.

    Raises:
        FileNotFoundError: If the query file cannot be found.
        AirflowException: If the Airflow connection retrieval fails.
        ConnectionError: If a network-level connection to ClickHouse cannot be established.
        ClickHouseError: If ClickHouse rejects the query or connection setup fails.
    """
    try:
        logging.info(f"Executing query from file: {query_file}")

        with open(query_file, "r") as f:
            query = f.read()
        logging.info("Query read successfully, executing...")

        with get_clickhouse_client() as client:
            records = client.query(query).result_rows

        logging.info(f"Query executed successfully, fetched {len(records)} records")
        return records

    except FileNotFoundError as fnf:
        logging.error(f"Query file not found: {fnf}")
        raise
    except  ConnectionError as ce:
        logging.error(f"Error querying ClickHouse: {ce}")
        raise

def transfer_clickhouse_to_sqlite():
    """
    Transfer data from ClickHouse to SQLite.
    This function queries data from ClickHouse using the query defined in
    `query_clickhouse`, and inserts the records into the `trip_metrics`
    table in SQLite.

    Raises:
        FileNotFoundError: If the query file cannot be found.
        sqlite3.Error: If an error occurs while inserting into SQLite.
    """
    try:
        logging.info("Starting data transfer from ClickHouse to SQLite")
        records = query_clickhouse()

        if not records:
            logging.warning("No records found in ClickHouse. Transfer aborted.")
            return

        sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_default")
        with sqlite_hook.get_conn() as conn:
            cursor = conn.cursor()

            logging.info(f"Inserting {len(records)} records into SQLite")
            insert_query = """
                INSERT INTO trip_metrics (
                    months, sat_mean_trip_count, sat_mean_fare_per_trip, sat_mean_duration_per_trip, 
                    sun_mean_trip_count, sun_mean_fare_per_trip, sun_mean_duration_per_trip
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """
            cursor.executemany(insert_query, records)
            conn.commit()

        logging.info("Data transfer to SQLite completed successfully")
    except sqlite3.Error as se:
        logging.error(f"SQLite error during data insertion: {se}")
        raise
    except FileNotFoundError as fnf:
        logging.error(f"Query file not found: {fnf}")
        raise