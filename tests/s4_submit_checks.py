# Databricks notebook source
# MAGIC %md
# MAGIC # r2x spike S4: who may submit runs as the client account
# MAGIC Throwaway. Mode `framework` (run as the stand-in framework account): submits the same request twice and gives a viewer group access. Mode `negative` (run as the client account): tries to start runs as the App and framework accounts, which must be refused.

# COMMAND ----------

dbutils.widgets.text("mode", "framework")
dbutils.widgets.text("git_commit", "1a3d6aa79818b21e861fd5be6c97aea880b1b236")
dbutils.widgets.text("viewer_group", "uc-etl-framework-readers-dev")
p = {k: dbutils.widgets.get(k) for k in ["mode", "git_commit", "viewer_group"]}

# COMMAND ----------

import json, uuid
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
who = spark.sql("SELECT current_user()").first()[0]
T = "`sconset-db-01`.r2x_spike"
CLIENT, APP, FRAMEWORK = "9b365b7d-97fc-45ac-9d98-92fc1d6e723c", "5b819488-b2da-4cff-98b8-b30c2be10cb5", "a7025148-2cd1-4695-9bb9-72035b02337d"

def body(run_as, token):
    return {
        "run_name": f"r2x-spike-s4-{token[-8:]}",
        "idempotency_token": token,
        "run_as": {"service_principal_name": run_as},
        "access_control_list": [{"group_name": p["viewer_group"], "permission_level": "CAN_VIEW"},
                                {"user_name": "matt.raeber@integrate-solutions.com", "permission_level": "CAN_VIEW"}],
        "git_source": {"git_url": "https://github.com/m-raeber/databricks_framework_v2_spikes", "git_provider": "gitHub", "git_commit": p["git_commit"]},
        "tasks": [{"task_key": "client_step", "notebook_task": {"notebook_path": "tests/s1_client_step", "source": "GIT",
                   "base_parameters": {"request_id": token, "input_table": f"{T}.input_sample", "input_version": "1", "staging_table": f"{T}.staging_out"}}}],
    }

def try_submit(run_as, token):
    try:
        return {"ok": True, "run_id": w.api_client.do("POST", "/api/2.2/jobs/runs/submit", body=body(run_as, token))["run_id"]}
    except Exception as e:
        return {"ok": False, "error": str(e).split("\n")[0][:300]}

out = {"mode": p["mode"], "ran_as": who}
if p["mode"] == "framework":
    token = f"r2x-s4-{uuid.uuid4().hex[:8]}"
    first, second = try_submit(CLIENT, token), try_submit(CLIENT, token)
    out.update({"token": token, "first_submit": first, "second_submit_same_token": second,
                "same_run": first.get("run_id") == second.get("run_id")})
    if first.get("ok"):
        run = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": first["run_id"]})
        perms = w.api_client.do("GET", f"/api/2.0/permissions/jobs/{run['job_id']}")
        out["run_permissions"] = [(a.get("group_name") or a.get("user_name") or a.get("service_principal_name"),
                                   [x["permission_level"] for x in a["all_permissions"]]) for a in perms["access_control_list"]]
        out["run_creator"] = run.get("creator_user_name")
else:
    out["client_as_app"] = try_submit(APP, f"r2x-s4neg-{uuid.uuid4().hex[:8]}")
    out["client_as_framework"] = try_submit(FRAMEWORK, f"r2x-s4neg-{uuid.uuid4().hex[:8]}")
print(json.dumps(out, indent=2))
dbutils.notebook.exit(json.dumps(out))
