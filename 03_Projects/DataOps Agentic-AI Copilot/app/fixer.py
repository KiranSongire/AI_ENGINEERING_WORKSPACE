def generate_fix_for_log(error_type: str):
    mapping = {
        "DUPLICATE_KEY": {
            "root_cause": "Duplicate records are entering the load step without deduplication.",
            "suggested_fix": "Add pre-load deduplication on txn_id or use UPSERT logic.",
            "action_plan": [
                "Inspect duplicate source rows",
                "Deduplicate records by txn_id before load",
                "Re-run failed batch in lower environment"
            ]
        },
        "MISSING_COLUMN": {
            "root_cause": "Transformation logic references a column not present in the incoming schema.",
            "suggested_fix": "Update mapping or add schema validation before transformation.",
            "action_plan": [
                "Compare source schema with expected schema",
                "Fix transformation column mapping",
                "Add schema drift check"
            ]
        },
        "TIMEOUT": {
            "root_cause": "Load step likely exceeded connection or query execution limit.",
            "suggested_fix": "Retry with smaller batch size or improve DB/query performance.",
            "action_plan": [
                "Check target DB load performance",
                "Reduce batch size",
                "Add retry with backoff"
            ]
        },
        "NULL_SPIKE": {
            "root_cause": "Unexpected increase in null values indicates ingestion or transformation issue.",
            "suggested_fix": "Block downstream load and trace upstream field population.",
            "action_plan": [
                "Identify upstream source for amount field",
                "Validate transformation mapping",
                "Set null threshold alert"
            ]
        },
        "SCHEMA_DRIFT": {
            "root_cause": "Incoming payload changed from expected structure.",
            "suggested_fix": "Add schema evolution handling or reject unexpected payloads safely.",
            "action_plan": [
                "Review new schema changes",
                "Update parser and contracts",
                "Add schema compatibility checks"
            ]
        },
        "TYPE_MISMATCH": {
            "root_cause": "Incoming data type does not match expected target schema.",
            "suggested_fix": "Add type casting or validation before loading.",
            "action_plan": [
                "Inspect source data types",
                "Update transformation logic",
                "Add pre-load validation checks"
            ]
        },
        "OUTLIER_SPIKE": {
            "root_cause": "Anomalous value spike suggests bad source data or duplicate aggregation.",
            "suggested_fix": "Quarantine anomalous rows and validate source calculations.",
            "action_plan": [
                "Identify outlier-producing rows",
                "Check upstream aggregation logic",
                "Set anomaly thresholds"
            ]
        }
    }

    return mapping.get(error_type, {
        "root_cause": "Unknown issue.",
        "suggested_fix": "Manually inspect the pipeline step.",
        "action_plan": [
            "Collect logs",
            "Review failing step",
            "Validate source and target states"
        ]
    })