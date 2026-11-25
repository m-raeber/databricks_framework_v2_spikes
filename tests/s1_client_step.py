# Databricks notebook source
# MAGIC %md
# MAGIC # r2x spike S1: client step
# MAGIC Throwaway. Runs as the client account in a one-time run whose code comes from GitHub at a fixed commit. Reads the pinned input version, writes a total per region to staging, and (for the spike only) tries to write to the framework table, which must be refused. Defaults point to Matt's sandbox, so running it by hand touches nothing else.

# COMMAND ----------

dbutils.widgets.text("request_id", "manual-try")
dbutils.widgets.text("input_table", "`sconset-db-01`.r2x_spike.input_sample")
dbutils.widgets.text("input_version", "")
dbutils.widgets.text("staging_table", "`sconset-db-01`.sandbox_matt.staging_out")
p = {k: dbutils.widgets.get(k) for k in ["request_id", "input_table", "input_version", "staging_table"]}

# COMMAND ----------

import json
who = spark.sql("SELECT current_user()").first()[0]
version_clause = f" VERSION AS OF {int(p['input_version'])}" if p["input_version"] else ""
df = spark.sql(f"SELECT region, SUM(amount) AS total FROM {p['input_table']}{version_clause} GROUP BY region")
rows = df.count()
df.createOrReplaceTempView("totals")
spark.sql(f"INSERT INTO {p['staging_table']} SELECT :rid, region, total, current_timestamp() FROM totals", args={"rid": p["request_id"]})

try:
    spark.sql("INSERT INTO `sconset-db-01`.r2x_spike.framework_evidence VALUES (:rid, 'client tried to write', current_timestamp())", args={"rid": p["request_id"]})
    fw = "ALLOWED"
except Exception as e:
    fw = "REFUSED: " + str(e).split("\n")[0][:200]

out = {"run_as": who, "params": p, "rows_written": rows, "write_framework_evidence": fw}
print(json.dumps(out, indent=2))
dbutils.notebook.exit(json.dumps(out))
