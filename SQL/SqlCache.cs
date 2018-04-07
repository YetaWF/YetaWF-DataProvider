/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using Microsoft.SqlServer.Management.Smo;
using YetaWF.Core.Support;
using System.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;

namespace YetaWF.DataProvider.SQL {

    /// <summary>
    /// Used to cache db info (table names, column names) as these are costly operations and the table names/column names are quite static.
    /// </summary>
    /// <remarks>To minimize startup time, we collect table names and column names as they are accessed the first time (lazy loading).</remarks>
    public static class SQLCache {

        private class DBEntry {
            public Dictionary<string, TableEntry> Tables { get; set; }
            public DBEntry() {
                Tables = new Dictionary<string, TableEntry>();
            }
        }
        private class TableEntry {
            public List<string> Columns { get; set; }
            public TableEntry() {
                Columns = new List<string>();
            }
        }

        private static Dictionary<string, DBEntry> Databases = new Dictionary<string, DBEntry>();

        private static Database GetDatabase(SqlConnection conn) {
            Server server = new Server(new ServerConnection(conn));
            if (server.Databases == null || !server.Databases.Contains(conn.Database))
                throw new InternalError("Can't connect to database {0}", conn.Database);
            Database db = server.Databases[conn.Database];
            if (!Databases.ContainsKey(db.Name)) {
                try {
                    Databases.Add(db.Name, new DBEntry());
                } catch (Exception) { }// can fail if duplicate added (we prefer not to lock)
            }
            return db;
        }
        /// <summary>
        /// Clear the cache.
        /// </summary>
        internal static void ClearCache() {
            Databases = new Dictionary<string, DBEntry>();
        }

        internal static bool HasTable(SqlConnection conn, string databaseName, string tableName) {
            DBEntry dbEntry;
            Database db = null;
            if (!Databases.TryGetValue(databaseName, out dbEntry)) {
                db = GetDatabase(conn);// we need to cache it now
                dbEntry = Databases[databaseName];
            }
            // check if we already have this table cached
            if (!dbEntry.Tables.ContainsKey(tableName)) {
                // we don't so add it to cache now
                if (db == null)
                    db = GetDatabase(conn);
                Table table = db.Tables[tableName];
                if (table == null)
                    return false;
                try {
#if DEBUG
                    if (!dbEntry.Tables.ContainsKey(tableName)) // minimize exception spam
#endif
                       dbEntry.Tables.Add(tableName, new TableEntry { });
                } catch (Exception) { }// can fail if duplicate added (we prefer not to lock)
            }
            return true;
        }
        public static List<string> GetColumns(SqlConnection conn, string databaseName, string tableName) {
            DBEntry dbEntry;
            Database db = null;
            if (!Databases.TryGetValue(databaseName, out dbEntry)) {
                db = GetDatabase(conn);// we need to cache it now
                dbEntry = Databases[databaseName];
            }
            // check if we already have this table cached
            TableEntry tableEntry;
            Table table = null;
            if (!dbEntry.Tables.TryGetValue(tableName, out tableEntry)) {
                // we don't so add it to cache now
                if (db == null)
                    db = GetDatabase(conn);
                table = db.Tables[tableName];
                if (table == null)
                    throw new InternalError("Request for db {0} table {1} which doesn't exist", databaseName, tableName);
                tableEntry = new TableEntry { };
                try {
#if DEBUG // minimize exception spam
                    if (!dbEntry.Tables.ContainsKey(tableName))
#endif
                        dbEntry.Tables.Add(tableName, tableEntry);
                } catch (Exception) {// can fail if duplicate added (we prefer not to lock)
                    tableEntry = dbEntry.Tables[tableName];// if we had a dup, make sure to get the real entry
                }
            }
            if (tableEntry.Columns.Count == 0) {
                // we don't have the columns yet
                List<string> cols = new List<string>();
                if (table == null) {
                    if (db == null)
                        db = GetDatabase(conn);
                    table = db.Tables[tableName];
                }
                foreach (Column c in table.Columns)
                    cols.Add(c.Name);
                tableEntry.Columns = cols;
            }
            return tableEntry.Columns;
        }
    }
}
