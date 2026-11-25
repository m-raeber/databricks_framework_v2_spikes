-- r2x spike S3/S12: counts step, a SQL file. Uses the values the framework passes directly.
-- To try it by hand in the SQL editor, fill the parameter boxes from the sandbox section of process.yml:
--   request_id = manual-try, input_sales = `sconset-db-01`.r2x_spike.input_sample, input_sales_version = 1,
--   output_counts = `sconset-db-01`.sandbox_matt.staging_other, setting_min_amount = 0

INSERT INTO IDENTIFIER(:output_counts)
SELECT :request_id, region, CAST(COUNT(*) AS DECIMAL(18,2)), current_timestamp()
FROM IDENTIFIER(:input_sales) VERSION AS OF :input_sales_version
WHERE amount >= CAST(:setting_min_amount AS DECIMAL(18,2))
GROUP BY region;
