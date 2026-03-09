import sqlite3
import pandas as pd
from pathlib import Path

BASE_DIR= Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "app.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)

    transactions = pd.read_csv(DATA_DIR / "transactions.csv")

    transactions.to_sql(
        "transactions",
        conn,
        if_exists="replace",
        index=False
    )

    conn.commit()
    conn.close()


def run_query(sql):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(sql, conn)
    conn.close()

    return df