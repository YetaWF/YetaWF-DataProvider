/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using YetaWF.Core.Models;
using YetaWF.Core.Support;
using YetaWF.DataProvider.SQLGeneric;

namespace YetaWF.DataProvider.PostgreSQL {

    internal partial class SQLGen {

        public bool DropTable(string dbName, string schema, string tableName, List<string> errorList) {
            try {
                SQLManager.DropTable(Conn, dbName, schema, tableName);
                return true;
            } catch (Exception exc) {
                if (Logging) YetaWF.Core.Log.Logging.AddErrorLog($"Couldn't drop table {tableName}", exc);
                errorList.Add($"Couldn't drop table {tableName}");
                errorList.Add(ErrorHandling.FormatExceptionMessage(exc));
                return false;
            }
        }
        public bool DropSubTables(string dbName, string schema, string tableName, List<string> errorList) {
            bool status = true;
            SQLManager sqlManager = new SQLManager();
            string subtablePrefix = tableName + "_";
            List<SQLGenericGen.Table> tables = sqlManager.GetTables(Conn, dbName, schema);
            foreach (SQLGenericGen.Table table in tables) {
                if (table.Name.StartsWith(subtablePrefix))
                    if (!DropTable(dbName, schema, table.Name, errorList))
                        status = false;
            }
            return status;
        }
    }
}