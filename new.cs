using System;
using System.Data;
using System.Data.SqlClient;

// Add this file to the existing RetailFundingEndOfDay project.
//
// This version does NOT build DELETE statements in C#.
// dbo.spCMSPurgeData reads dbo.PurgeTableList, performs the batched deletes,
// and returns one result row per configured purge target.
//
// It intentionally relies on the project's existing:
// - Program.sqlServerRfdConn
// - SQLServerConnection wrapper
// - RetailFundingEndOfDay.Properties.Settings
// - Cryptography
// - WriteLineToLog
// - SendErrorEmail
// - logFilePath
// - racfId / racfPassword
partial class Program
{
    private const string PurgeStoredProcedureName = "dbo.spCMSPurgeData";

    /// <summary>
    /// Entry point for the retention-cleanup portion of the application.
    /// Call this from the existing Main/end-of-day workflow.
    /// </summary>
    public static void RunRetentionCleanupFromThisFile()
    {
        WriteLineToLog("Starting retention cleanup job");

        try
        {
            ConnectToDatabases();

            // Keep this call only if the surrounding end-of-day application
            // still needs RACF credentials for other work.
            GetRacfIdAndPassword();

            ExecuteCmsPurgeData();

            WriteLineToLog("Retention cleanup job completed");
        }
        catch (Exception ex)
        {
            WriteLineToLog(
                "Retention cleanup job failed. Message: " + ex.Message);

            if (ex.InnerException != null)
            {
                WriteLineToLog(
                    "Retention cleanup inner exception: " + ex.InnerException);
            }

            SendErrorEmail(
                "Error occurred during retention cleanup: " + logFilePath,
                logFilePath);

            throw;
        }
    }

    /// <summary>
    /// Executes dbo.spCMSPurgeData and processes the result set returned by it.
    ///
    /// Expected result columns:
    /// TableName, CriteriaColumn, RecordsPurged, IsSuccess, ErrorMessage
    /// </summary>
    private static void ExecuteCmsPurgeData()
    {
        string runId = Guid.NewGuid().ToString();

        int tablesProcessed = 0;
        int tablesWithDeletes = 0;
        int tablesFailed = 0;
        long totalRowsDeleted = 0;

        using (DataTable purgeResults = new DataTable())
        using (SqlCommand command = new SqlCommand())
        {
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = PurgeStoredProcedureName;

            // Purging can take longer than the default SQL command timeout.
            // Zero means no application-side timeout.
            command.CommandTimeout = 0;

            WriteLineToLog(
                $"Retention run {runId} started. Executing {PurgeStoredProcedureName}.");

            Program.sqlServerRfdConn.ExecuteSql(ref command, ref purgeResults);

            ValidatePurgeResultColumns(purgeResults);

            foreach (DataRow row in purgeResults.Rows)
            {
                tablesProcessed++;

                string tableName = GetRequiredString(row, "TableName");
                string criteriaColumn = GetRequiredString(row, "CriteriaColumn");
                long recordsPurged = GetInt64(row, "RecordsPurged");
                bool isSuccess = GetBoolean(row, "IsSuccess");
                string errorMessage = GetNullableString(row, "ErrorMessage");

                if (recordsPurged > 0)
                {
                    tablesWithDeletes++;
                    totalRowsDeleted += recordsPurged;
                }

                if (isSuccess)
                {
                    WriteLineToLog(
                        $"Retention run {runId}: table={tableName}, " +
                        $"criteriaColumn={criteriaColumn}, " +
                        $"recordsPurged={recordsPurged}, success=True.");
                }
                else
                {
                    tablesFailed++;

                    WriteLineToLog(
                        $"Retention run {runId}: table={tableName}, " +
                        $"criteriaColumn={criteriaColumn}, " +
                        $"recordsPurgedBeforeFailure={recordsPurged}, " +
                        $"success=False, error={errorMessage}.");
                }
            }
        }

        // This preserves the summary behavior from the earlier C# implementation.
        // It requires dbo.audit_log to contain the four referenced columns.
        InsertRetentionSummary(
            runId,
            tablesProcessed,
            tablesWithDeletes,
            tablesFailed,
            totalRowsDeleted);

        WriteLineToLog(
            $"Retention run {runId} completed. " +
            $"TablesProcessed={tablesProcessed}, " +
            $"TablesWithDeletes={tablesWithDeletes}, " +
            $"TablesFailed={tablesFailed}, " +
            $"TotalRowsDeleted={totalRowsDeleted}.");

        if (tablesFailed > 0)
        {
            throw new InvalidOperationException(
                $"{PurgeStoredProcedureName} reported failure for " +
                $"{tablesFailed} purge target(s). Review the application log.");
        }
    }

    /// <summary>
    /// Confirms that the stored procedure returned the columns expected by this code.
    /// </summary>
    private static void ValidatePurgeResultColumns(DataTable results)
    {
        string[] requiredColumns =
        {
            "TableName",
            "CriteriaColumn",
            "RecordsPurged",
            "IsSuccess",
            "ErrorMessage"
        };

        foreach (string columnName in requiredColumns)
        {
            if (!results.Columns.Contains(columnName))
            {
                throw new InvalidOperationException(
                    $"{PurgeStoredProcedureName} did not return required column " +
                    $"'{columnName}'. Check the final SELECT in the stored procedure.");
            }
        }
    }

    /// <summary>
    /// Writes one permanent run-level summary.
    ///
    /// Expected existing table:
    /// dbo.audit_log
    ///
    /// Required columns:
    /// RunId, TablesDeletedFrom, TotalRowsDeleted, CreatedAt
    ///
    /// The extra values TablesProcessed and TablesFailed are written to the normal
    /// application log because the previously used audit_log definition did not show
    /// columns for them.
    /// </summary>
    private static void InsertRetentionSummary(
        string runId,
        int tablesProcessed,
        int tablesWithDeletes,
        int tablesFailed,
        long totalRowsDeleted)
    {
        const string summarySql = @"
            INSERT INTO dbo.audit_log
            (
                RunId,
                TablesDeletedFrom,
                TotalRowsDeleted,
                CreatedAt
            )
            VALUES
            (
                @RunId,
                @TablesDeletedFrom,
                @TotalRowsDeleted,
                GETDATE()
            );";

        using (SqlCommand command = new SqlCommand())
        {
            command.CommandType = CommandType.Text;
            command.CommandText = summarySql;

            command.Parameters.Add(
                "@RunId",
                SqlDbType.VarChar,
                36).Value = runId;

            command.Parameters.Add(
                "@TablesDeletedFrom",
                SqlDbType.Int).Value = tablesWithDeletes;

            command.Parameters.Add(
                "@TotalRowsDeleted",
                SqlDbType.BigInt).Value = totalRowsDeleted;

            Program.sqlServerRfdConn.ExecuteSql(ref command);
        }

        WriteLineToLog(
            $"Retention run {runId}: permanent summary inserted into dbo.audit_log. " +
            $"TablesProcessed={tablesProcessed}, TablesFailed={tablesFailed}.");
    }

    private static string GetRequiredString(DataRow row, string columnName)
    {
        if (row.IsNull(columnName))
        {
            throw new InvalidOperationException(
                $"Stored procedure returned NULL for required column '{columnName}'.");
        }

        return Convert.ToString(row[columnName]).Trim();
    }

    private static string GetNullableString(DataRow row, string columnName)
    {
        return row.IsNull(columnName)
            ? string.Empty
            : Convert.ToString(row[columnName]).Trim();
    }

    private static long GetInt64(DataRow row, string columnName)
    {
        if (row.IsNull(columnName))
        {
            return 0;
        }

        return Convert.ToInt64(row[columnName]);
    }

    private static bool GetBoolean(DataRow row, string columnName)
    {
        if (row.IsNull(columnName))
        {
            return false;
        }

        return Convert.ToBoolean(row[columnName]);
    }

    static void ConnectToDatabases()
    {
        WriteLineToLog("Connecting to databases");

        try
        {
            string region =
                RetailFundingEndOfDay.Properties.Settings.Default
                    .RfdDbRegion
                    .Trim()
                    .ToLower();

            switch (region)
            {
                case "dev":
                    WriteLineToLog("Connecting to Dev RFD database");
                    sqlServerRfdConn = new SQLServerConnection(
                        Cryptography.DecryptString(
                            RetailFundingEndOfDay.Properties.Settings.Default
                                .RfdDbConnStringDev));
                    break;

                case "qv":
                    WriteLineToLog("Connecting to QV RFD database");
                    sqlServerRfdConn = new SQLServerConnection(
                        Cryptography.DecryptString(
                            RetailFundingEndOfDay.Properties.Settings.Default
                                .RfdDbConnStringQv));
                    break;

                case "dr":
                    WriteLineToLog("Connecting to DR RFD database");
                    sqlServerRfdConn = new SQLServerConnection(
                        Cryptography.DecryptString(
                            RetailFundingEndOfDay.Properties.Settings.Default
                                .RfdDbConnStringDr));
                    break;

                case "prod":
                    WriteLineToLog("Connecting to Prod RFD database");
                    sqlServerRfdConn = new SQLServerConnection(
                        Cryptography.DecryptString(
                            RetailFundingEndOfDay.Properties.Settings.Default
                                .RfdDbConnStringProd));
                    break;

                default:
                    throw new ApplicationException(
                        "Invalid RfdDbRegion in config file: " + region);
            }

            WriteLineToLog("Connecting to databases completed");
        }
        catch (Exception ex)
        {
            WriteLineToLog(
                "Error occurred while creating connection to database. " +
                "Error Message: " + ex.Message);

            SendErrorEmail(
                "Error occurred during End of Day execution at ConnectToDatabases.",
                logFilePath);

            throw;
        }
    }

    /// <summary>
    /// Gets RACFID and password for the existing mainframe workflow.
    /// This method is unrelated to the SQL purge itself.
    /// </summary>
    static void GetRacfIdAndPassword()
    {
        WriteLineToLog("Get RACF ID and password");

        using (DataTable result = new DataTable())
        using (SqlCommand command = new SqlCommand())
        {
            try
            {
                command.CommandType = CommandType.Text;
                command.CommandText =
                    "SELECT TOP (1) strUserId, strPassword FROM dbo.RFDUserID;";

                Program.sqlServerRfdConn.ExecuteSql(ref command, ref result);

                if (result.Rows.Count == 0)
                {
                    throw new InvalidOperationException(
                        "No RACF credentials were found in dbo.RFDUserID.");
                }

                racfId =
                    Convert.ToString(result.Rows[0]["strUserId"]).Trim();

                racfPassword = Cryptography.DecryptString(
                    Convert.ToString(result.Rows[0]["strPassword"]));

                WriteLineToLog("Get RACF ID and password completed");
            }
            catch (Exception ex)
            {
                WriteLineToLog(
                    "Error occurred while fetching RACF ID and password. " +
                    "Error Message: " + ex.Message);

                throw;
            }
        }
    }
}
