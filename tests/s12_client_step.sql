-- r2x spike S12: the same client step as s1_client_step, written as a SQL file.
-- The framework passes four values into the run. To try it out yourself in the
-- SQL editor, fill the parameter boxes with your sandbox values:
--   request_id    = manual-try
--   input_table   = `sconset-db-01`.r2x_spike.input_sample
--   input_version = 1
--   staging_table = `sconset-db-01`.sandbox_matt.staging_out

INSERT INTO IDENTIFIER(:staging_table)
SELECT :request_id, region, SUM(amount), current_timestamp()
FROM IDENTIFIER(:input_table) VERSION AS OF :input_version
GROUP BY region;
