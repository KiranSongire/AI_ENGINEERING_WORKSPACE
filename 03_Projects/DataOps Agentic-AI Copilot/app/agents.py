from app.tools import get_log_by_job
from app.fixer import generate_fix_for_log
from app.tools import (
    profile_transactions,
    preview_duplicates,
    preview_nulls,
    preview_outliers,
    get_log_by_job,
    execute_sql,
)

from app.tools import get_log_by_job

def data_quality_agent():
    profile = profile_transactions()

    issues = []
    preview = []

    if profile["duplicate_txn_id_count"] > 0:
        issues.append("Duplicate txn_id values found.")
        preview.extend(preview_duplicates())

    if profile["null_amount_count"] > 0:
        issues.append("Null values found in amount.")
        preview.extend(preview_nulls())

    if profile["large_amount_outlier_count"] > 0:
        issues.append("Large outliers found in amount.")
        preview.extend(preview_outliers())

    summary = " | ".join(issues) if issues else "No major quality issues found."

    return {
        "agent": "data_quality_agent",
        "summary": summary,
        "root_cause": "Likely ingestion or upstream transformation issues causing duplicates, nulls, or spikes.",
        "suggested_fix": "Review duplicate prevention, null handling, and outlier validation thresholds.",
        "action_plan": [
            "Remove or quarantine duplicate records",
            "Validate missing amount values upstream",
            "Add threshold alerts for anomalous amounts"
        ],
        "preview": preview[:10]
    }

def pipeline_debug_agent(job_id: str):
    log = get_log_by_job(job_id)

    if not log:
        return {
            "agent": "pipeline_debug_agent",
            "summary": f"No logs found for job_id={job_id}",
            "root_cause": None,
            "suggested_fix": None,
            "action_plan": [],
            "preview": []
        }

    fix = generate_fix_for_log(log["error_type"])

    return {
        "agent": "pipeline_debug_agent",
        "summary": f"Analyzed pipeline failure for {job_id}",
        "root_cause": fix["root_cause"],
        "suggested_fix": fix["suggested_fix"],
        "action_plan": fix["action_plan"],
        "preview": [log]
    }
    
    
def pipeline_debug_agent(job_id: str):
    log = get_log_by_job(job_id)

    if not log:
        return {
            "agent": "pipeline_debug_agent",
            "summary": f"No logs found for job_id={job_id}",
            "root_cause": None,
            "suggested_fix": None,
            "action_plan": [],
            "preview": []
        }

    fix = generate_fix_for_log(log["error_type"])

    return {
        "agent": "pipeline_debug_agent",
        "summary": f"Analyzed pipeline failure for {job_id}",
        "root_cause": fix["root_cause"],
        "suggested_fix": fix["suggested_fix"],
        "action_plan": fix["action_plan"],
        "preview": [log]
    }

def sql_query_agent(query: str):
    q = query.lower().strip()

    if "top" in q and "transactions" in q:
        sql = """
        SELECT customer_id, SUM(amount) AS total_amount
        FROM transactions
        WHERE amount IS NOT NULL
        GROUP BY customer_id
        ORDER BY total_amount DESC
        LIMIT 10
        """
    elif "duplicates" in q:
        sql = """
        SELECT txn_id, COUNT(*) AS cnt
        FROM transactions
        GROUP BY txn_id
        HAVING COUNT(*) > 1
        """
    elif "failed" in q:
        sql = """
        SELECT *
        FROM transactions
        WHERE status = 'failed'
        """
    else:
        sql = """
        SELECT *
        FROM transactions
        LIMIT 10
        """

    result = execute_sql(sql)

    return {
        "agent": "sql_query_agent",
        "summary": "Generated and executed SQL from natural language query.",
        "generated_sql": sql.strip(),
        "preview": result,
        "root_cause": None,
        "suggested_fix": None,
        "action_plan": []
    }