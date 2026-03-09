from fastapi import FastAPI
from app.agents import pipeline_debug_agent, data_quality_agent, sql_query_agent
from app.database import init_db

app = FastAPI(title="DataOps AI Copilot")


@app.on_event("startup")
def startup_event():
    init_db()


@app.get("/")
def root():
    return {"message": "DataOps AI Copilot is running"}


@app.get("/agent/pipeline/{job_id}")
def run_pipeline_agent(job_id: str):
    return pipeline_debug_agent(job_id)


@app.get("/agent/quality")
def run_quality_agent():
    return data_quality_agent()


@app.get("/agent/sql")
def run_sql_agent(query: str):
    return sql_query_agent(query)