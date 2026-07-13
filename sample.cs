static void qryDEAutoOldData()
{
    //sample retention targets
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
​
    string runId = Guid.NewGuid().ToString();
​
    using var transaction = conn.BeginTransaction();
​
    try
    {
        int totalRowsDeleted = 0;
        int tablesWithDeletes = 0;
​
        foreach (var target in retentionTargets)
        {
            string strSQL = $@"
                DELETE FROM [dbo].[{target.Table}]
                OUTPUT
                    @RunId,
                    '{target.Schema}',
                    '{target.Table}',
                    '{target.DateColumn}',
                    CAST(deleted.[{target.IdColumn}] AS varchar(100)),
                    GETDATE()
                WHERE [{target.DateColumn}] < DATEADD(YEAR, -10, GETDATE());
            ";
​
            SqlCommand command = new SqlCommand(strSQL, conn, transaction);
            command.CommandType = CommandType.Text;
            command.Parameters.AddWithValue("@RunId", runId);
​
            int rowsDeleted = Program.sqlServerRfdConn.ExecuteSql(ref command);
​
            if (rowsDeleted > 0)
            {
                tablesWithDeletes++;
                totalRowsDeleted += rowsDeleted;
            }
        }
​
    //from this table, x num of rows deleted, at y time, 
    //
        string summarySQL = @"
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
            );
        ";
​
        SqlCommand summaryCommand = new SqlCommand(summarySQL, conn, transaction);
        summaryCommand.CommandType = CommandType.Text;
        summaryCommand.Parameters.AddWithValue("@RunId", runId);
        summaryCommand.Parameters.AddWithValue("@TablesDeletedFrom", tablesWithDeletes);
        summaryCommand.Parameters.AddWithValue("@TotalRowsDeleted", totalRowsDeleted);
​
        Program.sqlServerRfdConn.ExecuteSql(ref summaryCommand);
​
        transaction.Commit();
    }
    catch (Exception ex)
    {
        transaction.Rollback();
​
        WriteLineToLog("Exception occurred while executing qryDEAutoOldData Message: " + ex.Message);
​
        if (ex.InnerException != null)
        {
            WriteLineToLog("InnerException: " + ex.InnerException.ToString());
            SendErrorEmail("Error occurred during End of Day execution at qryDEAutoOldData: " + logFilePath);
        }
    }
}
​
    static void ConnectToDatabases()
        {
            WriteLineToLog("Connecting to databases");
            try
            {
                // Changes end by REGHUGO - CHG0128845
                switch (RetailFundingEndOfDay.Properties.Settings.Default.RfdDbRegion.Trim().ToLower())
                {
                    case "dev":
                        WriteLineToLog("Connecting to Dev RFD database");
                        sqlServerRfdConn = new SQLServerConnection(Cryptography.DecryptString(RetailFundingEndOfDay.Properties.Settings.Default.RfdDbConnStringDev));
                        break;
                    case "qv":
                        WriteLineToLog("Connecting to QV RFD database");
                        sqlServerRfdConn = new SQLServerConnection(Cryptography.DecryptString(RetailFundingEndOfDay.Properties.Settings.Default.RfdDbConnStringQv));
                        break;
                    case "dr":
                        WriteLineToLog("Connecting to DR RFD database");
                        sqlServerRfdConn = new SQLServerConnection(Cryptography.DecryptString(RetailFundingEndOfDay.Properties.Settings.Default.RfdDbConnStringDr));
                        break;
                    case "prod":
                        WriteLineToLog("Connecting to Prod RFD database");
                        sqlServerRfdConn = new SQLServerConnection(Cryptography.DecryptString(RetailFundingEndOfDay.Properties.Settings.Default.RfdDbConnStringProd));
                        break;
                    default:
                        string errorMessage = "Invalid RfdDbRegion in config file: " + RetailFundingEndOfDay.Properties.Settings.Default.RfdDbRegion.Trim();
                        WriteLineToLog(errorMessage);
                        throw new ApplicationException(errorMessage);
                }
​
                WriteLineToLog("Connecting to databases completed");
            }
​
            catch (Exception ex)
            {
                WriteLineToLog("Error occured while creating connection to Database. Error Message: " + ex.Message);
                SendErrorEmail("Error occured during End of Day execution at ConnectToDatabase.", logFilePath);
            }
        }
​
        /// <summary>
        /// Get RACFID and PAssword to access mainframe
        /// </summary>
        static void GetRacfIdAndPassword()
        {
            WriteLineToLog("Get Racf Id and Password");
​
            DataTable dt = new DataTable();
            SqlCommand sqlCommand = new SqlCommand();
            try
            {
                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.CommandText = "Select  strUserId ,strPassword  from RFDUserID";
                Program.sqlServerRfdConn.ExecuteSql(ref sqlCommand, ref dt);
                racfId = dt.Rows[0]["strUserId"].ToString().Trim();
                racfPassword = Cryptography.DecryptString(dt.Rows[0]["strPassword"].ToString());
​
                WriteLineToLog("Get Racf Id and Password completed");
            }
            catch (Exception ex)
            {
                WriteLineToLog("Error occured while fetching Racfid and Password from database. Error Message: " + ex.Message);
            }
            finally
            {
                dt.Dispose();
                sqlCommand.Dispose();
            }
        }
