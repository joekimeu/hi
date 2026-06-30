public static void qryDEAutoOldData()
{
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
        ("dbo", "CIFTrack", "dteFundingDate", "cifTrackId"),
        ("dbo", "Audit", "AuditDate", "AuditId")
    };

    string runId = Guid.NewGuid().ToString();

    using var transaction = conn.BeginTransaction();

    try
    {
        int totalRowsDeleted = 0;
        int tablesWithDeletes = 0;

        foreach (var target in retentionTargets)
        {
            string strSQL = $@"
                DELETE FROM [{target.Schema}].[{target.Table}]
                OUTPUT
                    @RunId,
                    '{target.Schema}',
                    '{target.Table}',
                    '{target.DateColumn}',
                    CAST(deleted.[{target.IdColumn}] AS varchar(100)),
                    GETDATE()
                INTO [dbo].[audit_log]
                (
                    RunId,
                    SchemaName,
                    TableName,
                    DateColumnName,
                    DeletedRowId,
                    DeletedAt
                )
                WHERE [{target.DateColumn}] < DATEADD(YEAR, -10, GETDATE());
            ";

            SqlCommand command = new SqlCommand(strSQL, conn, transaction);
            command.CommandType = CommandType.Text;
            command.Parameters.AddWithValue("@RunId", runId);

            int rowsDeleted = Program.sqlServerRfdConn.ExecuteSql(ref command);

            if (rowsDeleted > 0)
            {
                tablesWithDeletes++;
                totalRowsDeleted += rowsDeleted;
            }
        }

        string summarySQL = @"
            INSERT INTO [dbo].[audit_log_summary]
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
            );
        ";

        SqlCommand summaryCommand = new SqlCommand(summarySQL, conn, transaction);
        summaryCommand.CommandType = CommandType.Text;
        summaryCommand.Parameters.AddWithValue("@RunId", runId);
        summaryCommand.Parameters.AddWithValue("@TablesDeletedFrom", tablesWithDeletes);
        summaryCommand.Parameters.AddWithValue("@TotalRowsDeleted", totalRowsDeleted);

        Program.sqlServerRfdConn.ExecuteSql(ref summaryCommand);

        transaction.Commit();
    }
    catch (Exception ex)
    {
        transaction.Rollback();

        WriteLineToLog("Exception occurred while executing qryDEAutoOldData Message: " + ex.Message);

        if (ex.InnerException != null)
        {
            WriteLineToLog("InnerException: " + ex.InnerException.ToString());
            SendErrorEmail("Error occurred during End of Day execution at qryDEAutoOldData: " + logFilePath);
        }
    }
}
