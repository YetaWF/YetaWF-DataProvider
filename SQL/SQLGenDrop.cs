/* Copyright © 2019 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using YetaWF.Core.Support;
using YetaWF.DataProvider.SQL;

namespace YetaWF.DataProvider {

    internal partial class SQLGen {

        public bool DropTable(string dbName, string dbo, string tableName, List<string> errorList) {
            try {
                SQLManager.DropTable(Conn, dbName, dbo, tableName);
                return true;
            } catch (Exception exc) {
                if (Logging) YetaWF.Core.Log.Logging.AddErrorLog($"Couldn't drop table {tableName}", exc);
                errorList.Add($"Couldn't drop table {tableName}" );
                errorList.Add(ErrorHandling.FormatExceptionMessage(exc));
                return false;
            }
        }
        public bool DropSubTables(string dbName, string dbo, string tableName, List<string> errorList) {
            bool status = true;
            string subtablePrefix = tableName + "_";
            List<SQLManager.Table> tables = SQLManager.GetTables(Conn, dbName, dbo);
            foreach (SQLManager.Table table in tables) {
                if (table.Name.StartsWith(subtablePrefix))
                    if (!DropTable(dbName, dbo, table.Name, errorList))
                        status = false;
            }
            return status;
        }
    }
}