# Databricks notebook source
# MAGIC %md
# MAGIC # r2x spike: access check
# MAGIC Throwaway. Shows which account the run uses and what it may read and write in `sconset-db-01`.r2x_spike. The client account should be allowed to read input_sample and write staging_out, and be refused on framework_evidence.

# COMMAND ----------

import json
who = spark.sql("SELECT current_user()").first()[0]
T = "`sconset-db-01`.r2x_spike"
checks = [
    ("read input_sample", f"SELECT count(*) FROM {T}.input_sample"),
    ("write staging_out", f"INSERT INTO {T}.staging_out VALUES ('access-check', 'x', 0, current_timestamp())"),
    ("write framework_evidence", f"INSERT INTO {T}.framework_evidence VALUES ('access-check', 'client account must be refused', current_timestamp())"),
]
out = {"run_as": who}
for name, sql in checks:
    try:
        spark.sql(sql).collect()
        out[name] = "ALLOWED"
    except Exception as e:
        out[name] = "REFUSED: " + str(e).split("\n")[0][:300]
print(json.dumps(out, indent=2))
dbutils.notebook.exit(json.dumps(out))
