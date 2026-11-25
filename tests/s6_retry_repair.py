# Databricks notebook source
# MAGIC %md
# MAGIC # r2x spike S6: retries and repair on a one-time run
# MAGIC Throwaway. Runs as the stand-in framework account. Submits a two-step client run: the first step retries once, the second fails. Then repairs only the failed step with a changed value and records what Databricks did.

# COMMAND ----------

import json, time, uuid
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
T = "`sconset-db-01`.r2x_spike"
STEPS = "/Users/matt.raeber@integrate-solutions.com/ETL Framework v2/databricks_framework_v2_spikes/tests/s6_steps"
rid = f"r2x-s6-{uuid.uuid4().hex[:8]}"

def wait(run_id):
    while True:
        r = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id, "include_history": "true"})
        if r["state"]["life_cycle_state"] in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            return r
        time.sleep(10)

def task_summary(r):
    return [{"task": t["task_key"], "attempt": t.get("attempt_number", 0), "result": t["state"].get("result_state"),
             "message": (t["state"].get("state_message") or "")[:120]} for t in r["tasks"]]

run_id = w.api_client.do("POST", "/api/2.2/jobs/runs/submit", body={
    "run_name": f"r2x-spike-s6-{rid[-8:]}",
    "idempotency_token": rid,
    "run_as": {"service_principal_name": "9b365b7d-97fc-45ac-9d98-92fc1d6e723c"},
    "access_control_list": [{"user_name": "matt.raeber@integrate-solutions.com", "permission_level": "CAN_VIEW"}],
    "tasks": [
        {"task_key": "flaky", "max_retries": 1, "min_retry_interval_millis": 0,
         "notebook_task": {"notebook_path": STEPS, "source": "WORKSPACE", "base_parameters": {"request_id": rid, "step": "flaky"}}},
        {"task_key": "fixable", "depends_on": [{"task_key": "flaky"}],
         "notebook_task": {"notebook_path": STEPS, "source": "WORKSPACE", "base_parameters": {"request_id": rid, "step": "fixable", "should_fail": "yes"}}},
    ]})["run_id"]
first = wait(run_id)
out = {"request_id": rid, "client_run_id": run_id, "first_result": first["state"].get("result_state"), "first_tasks": task_summary(first)}

# COMMAND ----------

try:
    rep = w.api_client.do("POST", "/api/2.2/jobs/runs/repair", body={
        "run_id": run_id, "rerun_tasks": ["fixable"], "notebook_params": {"should_fail": "no"}})
    out["repair_id"] = rep.get("repair_id")
    time.sleep(5)
    after = wait(run_id)
    out["after_repair_result"] = after["state"].get("result_state")
    out["after_repair_tasks"] = task_summary(after)
    out["repair_history"] = [{"type": h.get("type"), "result": h.get("state", {}).get("result_state")} for h in after.get("repair_history", [])]
except Exception as e:
    out["repair_error"] = str(e).split("\n")[0][:300]
out["staging_rows"] = [(r.region, r.n) for r in spark.sql(
    f"SELECT region, count(*) n FROM {T}.staging_out WHERE request_id = :r GROUP BY region ORDER BY region", args={"r": rid}).collect()]
print(json.dumps(out, indent=2))
dbutils.notebook.exit(json.dumps(out))
