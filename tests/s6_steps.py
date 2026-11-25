# Databricks notebook source
# MAGIC %md
# MAGIC # r2x spike S6: client steps that fail on purpose
# MAGIC Throwaway. `flaky` fails on its first attempt and succeeds on the retry. `fixable` fails while `should_fail` is `yes`, so it can only succeed after a repair with `should_fail` = `no`. Attempts are counted with marker rows in staging.

# COMMAND ----------

dbutils.widgets.text("request_id", "manual-try")
dbutils.widgets.text("step", "flaky")
dbutils.widgets.text("should_fail", "yes")
p = {k: dbutils.widgets.get(k) for k in ["request_id", "step", "should_fail"]}
T = "`sconset-db-01`.r2x_spike"

# COMMAND ----------

import json
if p["step"] == "flaky":
    before = spark.sql(f"SELECT count(*) FROM {T}.staging_out WHERE request_id = :r AND region = 's6-attempt'", args={"r": p["request_id"]}).first()[0]
    spark.sql(f"INSERT INTO {T}.staging_out VALUES (:r, 's6-attempt', 0, current_timestamp())", args={"r": p["request_id"]})
    if before == 0:
        raise Exception("Planned failure on the first attempt (S6)")
    result = {"step": "flaky", "attempt": before + 1}
else:
    if p["should_fail"] == "yes":
        raise Exception("Planned failure until repaired (S6)")
    spark.sql(f"INSERT INTO {T}.staging_out VALUES (:r, 's6-fixed', 1, current_timestamp())", args={"r": p["request_id"]})
    result = {"step": "fixable", "fixed": True}
dbutils.notebook.exit(json.dumps(result))
