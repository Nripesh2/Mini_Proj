-- One time execution
-- Changes the session timezone to Asia/Kolkata
ALTER SESSION SET TIMEZONE = 'Asia/Kolkata';
SELECT CURRENT_TIMESTAMP; -- Retrieves the current timestamp

-- Create or replace a JavaScript-based stored procedure
CREATE OR REPLACE PROCEDURE automate_pipeline_procedure()
	RETURNS STRING
	LANGUAGE JAVASCRIPT
	EXECUTE AS CALLER
	AS
	$$

		// Execute first stored procedure - EXECUTE_CSV_IMPORT()
		var stmt1 = snowflake.createStatement({sqlText: "CALL EXECUTE_CSV_IMPORT()"});
		var result1 = stmt1.execute();

		// Execute second stored procedure - EDH_PROCEDURE()
		var stmt2 = snowflake.createStatement({sqlText: "CALL EDH_PROCEDURE()"});
		var result2 = stmt2.execute();

		// Execute third stored procedure - AUDITLOG()
		var stmt3 = snowflake.createStatement({sqlText: "CALL AUDITLOG()"});
		var result3 = stmt3.execute();

		return 'Procedures executed successfully';
	$$;

-- Call the created procedure explicitly
CALL automate_pipeline_procedure();

-- Create a task that calls the above procedure
CREATE OR REPLACE TASK automated_pipeline_task
  WAREHOUSE = compute_wh
  SCHEDULE = '60 minute' -- Set the task to execute every 60 minutes
  AS
  CALL automate_pipeline_procedure(); -- Execute the stored procedure within the task


-- Auto-Resume task 'automated_pipeline_task' for every 60 minutes
ALTER TASK automated_pipeline_task RESUME;

-- Display a list of tasks executed
show tasks;