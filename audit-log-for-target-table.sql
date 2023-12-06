-- Creates an AuditTable to store audit log information
CREATE OR REPLACE TABLE AuditTable (
    Batch_ID STRING PRIMARY KEY, -- Unique identifier for each audit batch
    SP_Name STRING, -- Name of the stored procedure executed
    Status STRING, -- Execution status of the procedure
    Start_Time TIMESTAMP_NTZ, -- Start time of the procedure execution
    End_Time TIMESTAMP_NTZ, -- End time of the procedure execution
    Description STRING, -- Description or definition of the procedure
    SQL STRING, -- SQL query executed
    Rows_affected NUMBER, -- Number of rows affected by the query
    Executing_User STRING -- User executing the procedure
);

-- Creates a procedure to log audit information into the AuditTable
CREATE OR REPLACE PROCEDURE AuditLog()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS
$$
try {
	// Defines a merge query to update the AuditTable with audit information
    var mergeQuery = `
        MERGE INTO AuditTable AS T
        USING (
            SELECT
                CURRENT_TIMESTAMP() || '-' || ROW_NUMBER() OVER (ORDER BY qh.start_time DESC) AS Batch_ID, // Generates a unique Batch_ID
                p.procedure_name AS SP_Name, // Retrieves procedure name
                qh.execution_status AS Status, // Retrieves execution status
                qh.start_time AS Start_Time, // Retrieves start time of execution
                qh.end_time AS End_Time, // Retrieves end time of execution
                p.procedure_definition AS Description, // Retrieves procedure definition
                qh.QUERY_TEXT AS SQL, // Retrieves executed SQL query
                qh.ROWS_PRODUCED AS Rows_affected, // Retrieves the number of affected rows
                qh.user_name AS Executing_User // Retrieves the executing user
            FROM
                table(information_schema.query_history()) qh // Retrieves query history information
            JOIN
                information_schema.procedures p 
            ON
                p.PROCEDURE_OWNER = qh.ROLE_NAME // Joins with procedure info
            ORDER BY
                qh.start_time DESC // Orders by start time in descending order
        ) AS S
        ON T.Batch_ID = S.Batch_ID
        WHEN NOT MATCHED THEN
            INSERT (Batch_ID, SP_Name, Status, Start_Time, End_Time, Description, SQL, Rows_affected, Executing_User)
            VALUES (S.Batch_ID, S.SP_Name, S.Status, S.Start_Time, S.End_Time, S.Description, S.SQL, S.Rows_affected, S.Executing_User);
    `;

	// Executes the merge query to update the AuditTable
    var mergeStmt = snowflake.createStatement({sqlText: mergeQuery});
    mergeStmt.execute();

	// Returns success message if the procedure executed without errors
    return 'Procedure executed successfully';
} catch (err) {
	 // Returns an error message if an error occurred during execution
    return 'Error: ' + err.message;
}
$$;


-- One time execution
-- Changes the session timezone to Asia/Kolkata
ALTER SESSION SET TIMEZONE = 'Asia/Kolkata';
SELECT CURRENT_TIMESTAMP; -- Retrieves the current timestamp

CALL EDH_PROCEDURE(); -- Calls the EDH procedure to update the TargetTable
CALL AuditLog(); -- Calls the AuditLog procedure to update the AuditTable

-- Retrieves all records from the AuditTable
SELECT * FROM AuditTable; 