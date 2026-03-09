import streamlit as st
import requests
import pandas as pd

API_URL = "http://127.0.0.1:8000"

st.set_page_config(page_title="DataOps AI Copilot", layout="wide")
st.title("DataOps AI Copilot")


def show_backend_status():
    try:
        response = requests.get(f"{API_URL}/", timeout=5)
        response.raise_for_status()
        st.success("Backend connected successfully.")
    except requests.exceptions.RequestException as e:
        st.error(f"Backend not reachable: {e}")


def safe_get_json(url: str, params=None):
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


show_backend_status()

tab1, tab2, tab3 = st.tabs([
    "Pipeline Debug Agent",
    "Data Quality Agent",
    "SQL Query Agent"
])

with tab1:
    st.subheader("Pipeline Debug Agent")
    job_id = st.text_input("Enter Job ID", value="job_101", key="pipeline_job_id")

    if st.button("Analyze Pipeline Failure", key="pipeline_btn"):
        try:
            data = safe_get_json(f"{API_URL}/agent/pipeline/{job_id}")

            st.subheader("Summary")
            st.write(data.get("summary"))

            st.subheader("Root Cause")
            st.write(data.get("root_cause"))

            st.subheader("Suggested Fix")
            st.write(data.get("suggested_fix"))

            st.subheader("Action Plan")
            st.write(data.get("action_plan"))

            preview = data.get("preview", [])
            if preview:
                st.subheader("Log Preview")
                st.dataframe(pd.DataFrame(preview), use_container_width=True)

        except requests.exceptions.RequestException as e:
            st.error(f"Pipeline request failed: {e}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")

with tab2:
    st.subheader("Data Quality Agent")

    if st.button("Run Data Quality Scan", key="quality_btn"):
        try:
            data = safe_get_json(f"{API_URL}/agent/quality")

            st.subheader("Summary")
            st.write(data.get("summary"))

            st.subheader("Root Cause")
            st.write(data.get("root_cause"))

            st.subheader("Suggested Fix")
            st.write(data.get("suggested_fix"))

            st.subheader("Action Plan")
            st.write(data.get("action_plan"))

            preview = data.get("preview", [])
            if preview:
                st.subheader("Data Preview")
                st.dataframe(pd.DataFrame(preview), use_container_width=True)

        except requests.exceptions.RequestException as e:
            st.error(f"Quality request failed: {e}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")

with tab3:
    st.subheader("SQL Query Agent")
    query = st.text_input(
        "Ask a data question",
        value="show duplicates in transactions",
        key="sql_query_input"
    )

    if st.button("Run Query", key="sql_btn"):
        try:
            data = safe_get_json(f"{API_URL}/agent/sql", params={"query": query})

            st.subheader("Generated SQL")
            st.code(data.get("generated_sql", ""), language="sql")

            preview = data.get("preview", [])
            if preview:
                st.subheader("Query Result")
                st.dataframe(pd.DataFrame(preview), use_container_width=True)

        except requests.exceptions.RequestException as e:
            st.error(f"SQL request failed: {e}")
        except Exception as e:
            st.error(f"Unexpected error: {e}")