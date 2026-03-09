import pandas as pd
from pathlib import Path
from app.database import run_query

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"


def _clean_records(df: pd.DataFrame):
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def load_transactions():
    return pd.read_csv(DATA_DIR / "transactions.csv")


def profile_transactions():
    df = load_transactions()

    duplicate_count = int(df.duplicated(subset=["txn_id"]).sum())
    null_amount_count = int(df["amount"].isna().sum())

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    outlier_count = int((df["amount"] > 10000).sum())

    return {
        "row_count": int(len(df)),
        "duplicate_txn_id_count": duplicate_count,
        "null_amount_count": null_amount_count,
        "large_amount_outlier_count": outlier_count,
    }


def preview_duplicates():
    df = load_transactions()
    dupes = df[df.duplicated(subset=["txn_id"], keep=False)]
    return _clean_records(dupes)


def preview_nulls():
    df = load_transactions()
    nulls = df[df["amount"].isna()]
    return _clean_records(nulls)


def preview_outliers():
    df = load_transactions()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    outliers = df[df["amount"] > 10000]
    return _clean_records(outliers)


def load_logs():
    return pd.read_csv(DATA_DIR / "pipeline_logs.csv")


def get_log_by_job(job_id: str):
    df = load_logs()
    match = df[df["job_id"] == job_id]

    if match.empty:
        return None

    row = match.iloc[[0]]
    return _clean_records(row)[0]


def execute_sql(sql: str):
    df = run_query(sql)
    return _clean_records(df)