/* Copyright © 2019 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.PostgreSQL {

    internal partial class PostgreSQLGen {

        public bool DropTable(string dbName, string schema, string tableName, List<string> errorList) {
            try {
                PostgreSQLManager.DropTable(Conn, dbName, schema, tableName);
                return true;
            } catch (Exception exc) {
                if (Logging) YetaWF.Core.Log.Logging.AddErrorLog($"Couldn't drop table {tableName}", exc);
                errorList.Add($"Couldn't drop table {tableName}" );
                errorList.Add(ErrorHandling.FormatExceptionMessage(exc));
                return false;
            }
        }
        public bool DropSubTables(string dbName, string schema, string tableName, List<string> errorList) {
            bool status = true;
            string subtablePrefix = tableName + "_";
            List<PostgreSQLManager.Table> tables = PostgreSQLManager.GetTables(Conn, dbName, schema);
            foreach (PostgreSQLManager.Table table in tables) {
                if (table.Name.StartsWith(subtablePrefix))
                    if (!DropTable(dbName, schema, table.Name, errorList))
                        status = false;
            }
            return status;
        }
    }
}