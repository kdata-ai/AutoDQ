"""
# Copied from lines 10-12 of AutoDQ/07_Anomaly Detection.py
"""
import mlflow.deployments
import json, re, logging

"""
# Copied from lines 84-150 of AutoDQ/07_Anomaly Detection.py
"""
def detect_anomalies_per_column(table_list):
    """Run anomaly detection per column → exactly one row per column."""
    logs = []
    for table in table_list:
        try:
            full_table = f"{source_schema}.{table}"
            df = spark.table(full_table)
            if df.count() == 0:
                print(f" Skipping {table} (empty).")
                continue

            sampled_df = dynamic_sample(df)

            # Loop through each column once
            for field in sampled_df.schema.fields:
                col_name = field.name
                col_type = field.dataType.simpleString()
                col_sample = sampled_df.select(col_name).toPandas().to_dict(orient="records")

                prompt = f"""
                You are an AI anomaly detection system. Analyze column `{col_name}` (type: {col_type}).
                Based on the sample, describe the **most relevant anomaly pattern** (nulls, outliers, unusual values).
                Respond ONLY with a single JSON object:
                {{
                    "reason": "text",
                    "severity": "Low/Medium/High",
                    "confidence_score": float
                }}
                Sample: {json.dumps(col_sample, default=convert_json_safe)}
                """

                response = client.predict(
                    endpoint=llm_endpoint,
                    inputs={"messages": [{"role": "user", "content": prompt}]}
                )

                content = response["choices"][0]["message"]["content"]
                match = re.search(r"\{.*\}", content.strip(), re.DOTALL)

                if match:
                    try:
                        parsed = json.loads(match.group(0))
                        logs.append({
                            "table": table,
                            "column": col_name,
                            "reason": parsed.get("reason"),
                            "severity": parsed.get("severity"),
                            "confidence_score": float(parsed.get("confidence_score", 0)),
                            "detected_at": datetime.now().isoformat()
                        })
                    except json.JSONDecodeError as json_err:
                        logging.error(f" JSON parsing failed for {table}.{col_name}: {json_err}")
                        logging.error(f"Raw AI content:\n{content}")
                else:
                    logs.append({
                        "table": table,
                        "column": col_name,
                        "reason": "No anomaly detected",
                        "severity": "Low",
                        "confidence_score": 0.0,
                        "detected_at": datetime.now().isoformat()
                    })

        except Exception as e:
            logging.error(f" Failed on {table}: {str(e)}")
    return logs


