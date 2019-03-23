/* Copyright © 2019 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.Linq;
using YetaWF.Core.Packages;
using YetaWF.Core.Support;
using YetaWF.DataProvider.SQL;

namespace YetaWF.DataProvider {

    internal partial class SQLGen {

        private static List<string> DBsCompleted;

        // Index removal (for upgrades only)

        protected void RemoveIndexesIfNeeded(string dbName, string dbo) {

            if (!Package.MajorDataChange) return;

            if (DBsCompleted == null) DBsCompleted = new List<string>();
            if (DBsCompleted.Contains(dbName)) return; // already done

            // do multiple passes until no more indexes available (we don't want to figure out the dependencies)
            int passes = 0;
            for ( ; ; ++passes) {
                int drop = 0;
                int failures = 0;
                foreach (SQLManager.Table table in SQLManager.GetTables(Conn, dbName, dbo)) {
                    int tableFailures = 0;
                    List<ForeignKey> foreignKeys = SQLManager.GetInfoForeignKeys(Conn, dbName, dbo, table.Name);
                    foreach (ForeignKey foreignKey in foreignKeys) {
                        try {
                            SQLManager.DropForeignKey(Conn, dbName, dbo, table.Name, foreignKey.Name);
                            ++drop;
                        } catch (Exception) { ++tableFailures; }
                    }
                    List<Index> indexes = SQLManager.GetInfoIndexes(Conn, dbName, dbo, table.Name);
                    foreach (Index index in indexes) {
                        switch (index.IndexType) {
                            case IndexType.Indexed:
                                try {
                                    SQLManager.DropIndex(Conn, dbName, dbo, table.Name, index.Name);
                                    ++drop;
                                } catch (Exception) { ++tableFailures; }
                                break;
                            case IndexType.UniqueKey:
                                try {
                                    SQLManager.DropUniqueKeyIndex(Conn, dbName, dbo, table.Name, index.Name);
                                    ++drop;
                                } catch (Exception) { ++tableFailures; }
                                break;
                            case IndexType.PrimaryKey:
                                try {
                                    SQLManager.DropPrimaryKeyIndex(Conn, dbName, dbo, table.Name, index.Name);
                                    ++drop;
                                } catch (Exception) { ++tableFailures; }
                                break;
                        }
                    }
                    if (tableFailures == 0) {
                        //$$$ foreach (Column column in table.Columns) {
                        //    if (column.DefaultConstraint != null)
                        //        column.DefaultConstraint.Drop();
                        //}
                        //table.Alter();
                    }
                    failures += tableFailures;
                }
                if (failures == 0)
                    break;// successfully removed everything
                if (drop == 0)
                    throw new InternalError("No index/foreign keys could be dropped on the last pass in DB {0}", dbName);
            }
            DBsCompleted.Add(dbName);
        }

        public void DropAllTables(string dbName, string dbo) {
            // don't do any logging here - we might be deleting the tables needed for logging
            int maxTimes = 5;
            List<SQLManager.Table> tables = SQLManager.GetTables(Conn, dbName, dbo);
            for (int time = maxTimes; time > 0 && tables.Count > 0; --time) {
                foreach (SQLManager.Table table in tables) {
                    try {
                        SQLManager.DropTable(Conn, dbName, dbo, table.Name);
                        tables = SQLManager.GetTables(Conn, dbName, dbo);// get new list
                    } catch (Exception) { }
                }
            }
            SQLManager.ClearCache();
        }
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