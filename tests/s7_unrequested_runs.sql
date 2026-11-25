-- r2x spike S7: list every run made as the client account in the last 30 days,
-- with who submitted it, the commit it used, and whether it has a request.
-- Any row with status 'NO REQUEST - investigate' ran outside the framework.
-- Source: system.access.audit (about 15 minutes behind real time).
WITH client_runs AS (
  SELECT request_params.jobRunId AS run_id,
         min(event_time) AS started_at,
         max_by(request_params.notebookPath, event_time) AS notebook_path
  FROM system.access.audit
  WHERE event_date >= current_date() - INTERVAL 30 DAYS
    AND service_name = 'jobs' AND action_name = 'runStart'
    AND request_params.runCreatorUserName = '9b365b7d-97fc-45ac-9d98-92fc1d6e723c'  -- svc-etl-client-dev
  GROUP BY 1),
submits AS (
  SELECT response.result AS resp, user_identity.email AS submitted_by
  FROM system.access.audit
  WHERE event_date >= current_date() - INTERVAL 30 DAYS
    AND service_name = 'jobs' AND action_name = 'submitRun')
SELECT c.run_id, c.started_at,
       regexp_extract(c.notebook_path, '_commits/([0-9a-f]{40})/', 1) AS commit_used,
       s.submitted_by, r.request_id,
       CASE WHEN r.request_id IS NULL THEN 'NO REQUEST - investigate' ELSE 'ok' END AS status
FROM client_runs c
LEFT JOIN `sconset-db-01`.r2x_spike.request r ON CAST(r.client_run_id AS STRING) = c.run_id
LEFT JOIN submits s ON s.resp LIKE concat('%', c.run_id, '%')
ORDER BY c.started_at
