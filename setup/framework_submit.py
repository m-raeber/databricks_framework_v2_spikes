# Databricks notebook source
# MAGIC %md
# MAGIC # r2x spike: stand-in framework job
# MAGIC Throwaway. Runs as the stand-in framework account. Records a request, pins the input version, submits a one-time run as the client account using code from GitHub at a fixed commit, waits, and records what Databricks says was used.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("test_name", "s1")
dbutils.widgets.text("git_url", "https://github.com/m-raeber/databricks_framework_v2_spikes")
dbutils.widgets.text("git_commit", "")
dbutils.widgets.text("notebook_path", "tests/s1_client_step")
dbutils.widgets.text("client_sp", "9b365b7d-97fc-45ac-9d98-92fc1d6e723c")
dbutils.widgets.text("framework_run_id", "")
dbutils.widgets.text("git_branch", "")
dbutils.widgets.text("viewer", "matt.raeber@integrate-solutions.com")
p = {k: dbutils.widgets.get(k) for k in ["test_name", "git_url", "git_commit", "git_branch", "notebook_path", "client_sp", "framework_run_id", "viewer"]}
assert bool(p["git_commit"]) != bool(p["git_branch"]), "give exactly one of git_commit or git_branch"

# COMMAND ----------

import json, uuid
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

T = "`sconset-db-01`.r2x_spike"
w = WorkspaceClient()
who = spark.sql("SELECT current_user()").first()[0]
request_id = f"r2x-{p['test_name']}-{uuid.uuid4().hex[:8]}"
input_version = spark.sql(f"DESCRIBE HISTORY {T}.input_sample LIMIT 1").first()["version"]
fw_run = int(p["framework_run_id"]) if p["framework_run_id"].isdigit() else None
spark.sql(f"INSERT INTO {T}.request VALUES (:rid, :t, :who, current_timestamp(), :fr, NULL)",
          args={"rid": request_id, "t": p["test_name"], "who": who, "fr": fw_run})

# COMMAND ----------

# DBTITLE 1,Submit client run from GitHub
waiter = w.jobs.submit(
    run_name=f"r2x-spike-{p['test_name']}-{request_id[-8:]}",
    idempotency_token=request_id,
    run_as=jobs.JobRunAs(service_principal_name=p["client_sp"]),
    git_source=jobs.GitSource(git_url=p["git_url"], git_provider=jobs.GitProvider.GIT_HUB,
                              git_commit=p["git_commit"] or None, git_branch=p["git_branch"] or None),
    access_control_list=[jobs.JobAccessControlRequest(user_name=p["viewer"], permission_level=jobs.JobPermissionLevel.CAN_VIEW)] if p["viewer"] else None,
    tasks=[jobs.SubmitTask(
        task_key="client_step",
        notebook_task=jobs.NotebookTask(
            notebook_path=p["notebook_path"], source=jobs.Source.GIT,
            base_parameters={"request_id": request_id, "input_table": f"{T}.input_sample",
                             "input_version": str(input_version), "staging_table": f"{T}.staging_out"}))],
)
client_run_id = waiter.run_id
spark.sql(f"UPDATE {T}.request SET client_run_id = :c WHERE request_id = :rid", args={"c": client_run_id, "rid": request_id})
try:
    run = waiter.result()
except Exception as e:
    run = w.jobs.get_run(client_run_id)

# COMMAND ----------

# DBTITLE 1,Record what was used
task = run.tasks[0]
try:
    task_out = w.jobs.get_run_output(task.run_id)
    client_result = task_out.notebook_output.result if task_out.notebook_output else None
    client_error = task_out.error
except Exception as e:
    client_result, client_error = None, str(e)[:300]
raw = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": client_run_id})
def find_git(o, path=""):
    hits = {}
    if isinstance(o, dict):
        for k, v in o.items():
            if "git" in k.lower() or "commit" in k.lower():
                hits[path + k] = v
            hits.update(find_git(v, path + k + "."))
    elif isinstance(o, list):
        for i, v in enumerate(o):
            hits.update(find_git(v, f"{path}{i}."))
    return hits
git_fields = find_git(raw)
snap = run.git_source.git_snapshot if run.git_source else None
out = {
    "framework_ran_as": who, "request_id": request_id, "client_run_id": client_run_id,
    "client_run_as": run.run_as_user_name if hasattr(run, "run_as_user_name") else None,
    "state": str(run.state.result_state), "state_message": run.state.state_message,
    "commit_requested": p["git_commit"], "branch_requested": p["git_branch"], "commit_used": snap.used_commit if snap else None,
    "git_fields_from_runs_get": git_fields,
    "input_version": int(input_version), "client_result": client_result, "client_error": client_error,
}
print(json.dumps(out, indent=2, default=str))
dbutils.notebook.exit(json.dumps(out, default=str))