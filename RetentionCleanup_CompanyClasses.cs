using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

// Add this file to the existing RetailFundingEndOfDay project.
// It intentionally relies on the project's existing Program fields/methods,
// SQLServerConnection wrapper, Settings, Cryptography, logging, and email code.
partial class Program
{
    private const int DefaultRetentionYears = 10;
    private const int DefaultDeleteBatchSize = 5000;

    /// <summary>
    /// Single entry method for running the functions in this file.
    /// Call this from the application's existing Main/end-of-day workflow.
    /// </summary>
    public static void RunRetentionCleanupFromThisFile()
    {
        WriteLineToLog("Starting retention cleanup job");

        ConnectToDatabases();
        GetRacfIdAndPassword();
        qryDEAutoOldData(DefaultRetentionYears, DefaultDeleteBatchSize);

        WriteLineToLog("Retention cleanup job completed");
    }

    /// <summary>
    /// Deletes records older than the configured retention period in manageable batches.
    /// Per-table results are written to the normal application log, and a run summary is
    /// inserted into dbo.audit_log.
    /// </summary>
    static void qryDEAutoOldData(
        int retentionYears = DefaultRetentionYears,
        int batchSize = DefaultDeleteBatchSize)
    {
        if (retentionYears <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(retentionYears), "Retention years must be greater than zero.");
        }

        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than zero.");
        }

        var retentionTargets = new List<(string Schema, string Table, string DateColumn, string IdColumn)>
        {
            ("dbo", "Audit", "auditDate", "auditId"),
            ("dbo", "AuditUser", "auditDate", "auditUserId"),
            ("dbo", "AMR171", "auditDate", "amr171Id"),
            ("dbo", "Acct_Detail", "eff_date", "acctDetailId"),
            ("dbo", "AutoAccts", "dteFundingDate", "autoAcctId"),
            ("dbo", "AutoAcctsCompleteArchive", "dteFundingDate", "autoAcctsCompleteArchiveId"),
            ("dbo", "AutoAdvance", "dteFundingDate", "autoAdvanceId"),
            ("dbo", "AutoALSPay", "dtePayoffDate", "autoALSPayId"),
            ("dbo", "AutoALSPayCompleteArchive", "dtePayoffDate", "autoALSPayCompleteArchiveId"),
            ("dbo", "AutoBillTrack", "dteFundingDate", "autoBillTrackId"),
            ("dbo", "CIFTrack", "dteFundingDate", "cifTrackId")
        };

        string runId = Guid.NewGuid().ToString();
        int totalRowsDeleted = 0;
        int tablesWithDeletes = 0;

        try
        {
            WriteLineToLog(
                $"Retention run {runId} started. RetentionYears={retentionYears}, BatchSize={batchSize}.");

            foreach (var target in retentionTargets)
            {
                ValidateRetentionTarget(target.Schema, target.Table, target.DateColumn, target.IdColumn);

                int rowsDeletedFromTable = 0;
                int rowsDeletedInBatch;

                do
                {
                    string deleteSql = $@"
                        DELETE TOP (@BatchSize)
                        FROM {QuoteIdentifier(target.Schema)}.{QuoteIdentifier(target.Table)}
                        WHERE {QuoteIdentifier(target.DateColumn)} < DATEADD(YEAR, -@RetentionYears, GETDATE());";

                    using (SqlCommand command = new SqlCommand())
                    {
                        command.CommandType = CommandType.Text;
                        command.CommandText = deleteSql;
                        command.CommandTimeout = 0;
                        command.Parameters.Add("@BatchSize", SqlDbType.Int).Value = batchSize;
                        command.Parameters.Add("@RetentionYears", SqlDbType.Int).Value = retentionYears;

                        rowsDeletedInBatch = Program.sqlServerRfdConn.ExecuteSql(ref command);
                    }

                    if (rowsDeletedInBatch > 0)
                    {
                        rowsDeletedFromTable += rowsDeletedInBatch;
                        totalRowsDeleted += rowsDeletedInBatch;
                    }
                }
                while (rowsDeletedInBatch == batchSize);

                if (rowsDeletedFromTable > 0)
                {
                    tablesWithDeletes++;
                }

                WriteLineToLog(
                    $"Retention run {runId}: deleted {rowsDeletedFromTable} row(s) from " +
                    $"{target.Schema}.{target.Table} using {target.DateColumn}.");
            }

            InsertRetentionSummary(runId, tablesWithDeletes, totalRowsDeleted);

            WriteLineToLog(
                $"Retention run {runId} completed. TablesDeletedFrom={tablesWithDeletes}, " +
                $"TotalRowsDeleted={totalRowsDeleted}.");
        }
        catch (Exception ex)
        {
            WriteLineToLog(
                "Exception occurred while executing qryDEAutoOldData. Message: " + ex.Message);

            if (ex.InnerException != null)
            {
                WriteLineToLog("InnerException: " + ex.InnerException);
            }

            SendErrorEmail(
                "Error occurred during End of Day execution at qryDEAutoOldData: " + logFilePath,
                logFilePath);

            throw;
        }
    }

    /// <summary>
    /// Confirms that each hard-coded retention table and column exists before deletion.
    /// </summary>
    private static void ValidateRetentionTarget(
        string schemaName,
        string tableName,
        string dateColumn,
        string idColumn)
    {
        const string validationSql = @"
            SELECT COUNT(1) AS MatchCount
            FROM sys.tables t
            INNER JOIN sys.schemas s
                ON s.schema_id = t.schema_id
            WHERE s.name = @SchemaName
              AND t.name = @TableName
              AND EXISTS
              (
                  SELECT 1
                  FROM sys.columns c
                  WHERE c.object_id = t.object_id
                    AND c.name = @DateColumn
              )
              AND EXISTS
              (
                  SELECT 1
                  FROM sys.columns c
                  WHERE c.object_id = t.object_id
                    AND c.name = @IdColumn
              );";

        using (DataTable result = new DataTable())
        using (SqlCommand command = new SqlCommand())
        {
            command.CommandType = CommandType.Text;
            command.CommandText = validationSql;
            command.Parameters.Add("@SchemaName", SqlDbType.NVarChar, 128).Value = schemaName;
            command.Parameters.Add("@TableName", SqlDbType.NVarChar, 128).Value = tableName;
            command.Parameters.Add("@DateColumn", SqlDbType.NVarChar, 128).Value = dateColumn;
            command.Parameters.Add("@IdColumn", SqlDbType.NVarChar, 128).Value = idColumn;

            Program.sqlServerRfdConn.ExecuteSql(ref command, ref result);

            bool isValid = result.Rows.Count > 0 &&
                           Convert.ToInt32(result.Rows[0]["MatchCount"]) == 1;

            if (!isValid)
            {
                throw new InvalidOperationException(
                    $"Retention target is invalid or missing: {schemaName}.{tableName}, " +
                    $"date column {dateColumn}, ID column {idColumn}.");
            }
        }
    }

    private static void InsertRetentionSummary(
        string runId,
        int tablesWithDeletes,
        int totalRowsDeleted)
    {
        const string summarySql = @"
            INSERT INTO [dbo].[audit_log]
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

        using (SqlCommand summaryCommand = new SqlCommand())
        {
            summaryCommand.CommandType = CommandType.Text;
            summaryCommand.CommandText = summarySql;
            summaryCommand.Parameters.Add("@RunId", SqlDbType.VarChar, 36).Value = runId;
            summaryCommand.Parameters.Add("@TablesDeletedFrom", SqlDbType.Int).Value = tablesWithDeletes;
            summaryCommand.Parameters.Add("@TotalRowsDeleted", SqlDbType.Int).Value = totalRowsDeleted;

            Program.sqlServerRfdConn.ExecuteSql(ref summaryCommand);
        }
    }

    private static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new ArgumentException("SQL identifier cannot be empty.", nameof(identifier));
        }

        return "[" + identifier.Replace("]", "]]" ) + "]";
    }

    static void ConnectToDatabases()
    {
        WriteLineToLog("Connecting to databases");

        try
        {
            switch (RetailFundingEndOfDay.Properties.Settings.Default.RfdDbRegion.Trim().ToLower())
            {
                case "dev":
                    WriteLineToLog("Connecting to Dev RFD database");
                    sqlServerRfdConn = new SQLServerConnection(
                        Cryptography.DecryptString(
                            RetailFundingEndOfDay.Properties.Settings.Default.RfdDbConnStringDev));
                    break;

                case "qv":
                    WriteLineToLog("Connecting to QV RFD database");
                    sqlServerRfdConn = new SQLServerConnection(
                        Cryptography.DecryptString(
                            RetailFundingEndOfDay.Properties.Settings.Default.RfdDbConnStringQv));
                    break;

                case "dr":
                    WriteLineToLog("Connecting to DR RFD database");
                    sqlServerRfdConn = new SQLServerConnection(
                        Cryptography.DecryptString(
                            RetailFundingEndOfDay.Properties.Settings.Default.RfdDbConnStringDr));
                    break;

                case "prod":
                    WriteLineToLog("Connecting to Prod RFD database");
                    sqlServerRfdConn = new SQLServerConnection(
                        Cryptography.DecryptString(
                            RetailFundingEndOfDay.Properties.Settings.Default.RfdDbConnStringProd));
                    break;

                default:
                    string region = RetailFundingEndOfDay.Properties.Settings.Default.RfdDbRegion.Trim();
                    string errorMessage = "Invalid RfdDbRegion in config file: " + region;
                    WriteLineToLog(errorMessage);
                    throw new ApplicationException(errorMessage);
            }

            WriteLineToLog("Connecting to databases completed");
        }
        catch (Exception ex)
        {
            WriteLineToLog(
                "Error occurred while creating connection to Database. Error Message: " + ex.Message);
            SendErrorEmail(
                "Error occurred during End of Day execution at ConnectToDatabase.",
                logFilePath);
            throw;
        }
    }

    /// <summary>
    /// Gets RACFID and password to access the mainframe.
    /// </summary>
    static void GetRacfIdAndPassword()
    {
        WriteLineToLog("Get Racf Id and Password");

        using (DataTable dt = new DataTable())
        using (SqlCommand sqlCommand = new SqlCommand())
        {
            try
            {
                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.CommandText =
                    "SELECT TOP (1) strUserId, strPassword FROM dbo.RFDUserID;";

                Program.sqlServerRfdConn.ExecuteSql(ref sqlCommand, ref dt);

                if (dt.Rows.Count == 0)
                {
                    throw new InvalidOperationException("No RACF credentials were found in dbo.RFDUserID.");
                }

                racfId = Convert.ToString(dt.Rows[0]["strUserId"]).Trim();
                racfPassword = Cryptography.DecryptString(
                    Convert.ToString(dt.Rows[0]["strPassword"]));

                WriteLineToLog("Get Racf Id and Password completed");
            }
            catch (Exception ex)
            {
                WriteLineToLog(
                    "Error occurred while fetching RACF ID and password from database. " +
                    "Error Message: " + ex.Message);
                throw;
            }
        }
    }
}
