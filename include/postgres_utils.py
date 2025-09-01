import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def query_postgres_to_dataframe(conn_id: str, sql: str) -> pd.DataFrame:
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    df = pd.read_sql(sql, conn)
    return df