/* Copyright © 2019 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Npgsql;
using System;
using System.Collections.Generic;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.PostgreSQL {

    /// <summary>
    /// Used to cache db info (table names, column names) as these are costly operations and the table names/column names are quite static.
    /// </summary>
    /// <remarks>To minimize startup time, we collect table names and column names as they are accessed the first time (lazy loading).</remarks>
    internal static class PostgreSQLCache {

        public class DBEntry {
            public Dictionary<string, TableEntry> Tables { get; set; }
            public DBEntry() {
                Tables = new Dictionary<string, TableEntry>();
            }
        }
        public class TableEntry {
            public List<string> Columns { get; set; }
            public TableEntry() {
                Columns = new List<string>();
            }
        }

        private static Dictionary<string, DBEntry> Databases = new Dictionary<string, DBEntry>();

        internal static DBEntry GetDatabase(NpgsqlConnection conn, string connectionString) {
            if (Databases.Count == 0) {
                // retrieve all databases
                // SELECT datname FROM pg_database WHERE NOT datistemplate;
                using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                    cmd.Connection = conn;
                    cmd.CommandText = "SELECT datname FROM pg_database WHERE NOT datistemplate";
                    using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
                        while (rdr.Read()) {
                            string dbName = rdr.GetString(0);
                            if (!Databases.ContainsKey(dbName)) {
                                try {
                                    Databases.Add(dbName, new DBEntry());
                                } catch (Exception) { }// can fail if duplicate added (we prefer not to lock)
                            }
                        }
                    }
                }
            }
            DBEntry db;
            if (!Databases.TryGetValue(conn.Database, out db))
                throw new InternalError("Can't connect to database {0}", conn.Database);
            return db;
        }
        /// <summary>
        /// Clear the cache.
        /// </summary>
        internal static void ClearCache() {
            Databases = new Dictionary<string, DBEntry>();
        }

        internal static bool HasTable(NpgsqlConnection conn, string connectionString, string databaseName, string tableName) {
            DBEntry dbEntry;
            if (!Databases.TryGetValue(databaseName, out dbEntry))
                dbEntry = GetDatabase(conn, connectionString);// we need to cache it now

            // check if we already have this table cached
            if (!dbEntry.Tables.ContainsKey(tableName)) {
                // we don't so add it to cache now
                try {
#if DEBUG
                    if (!dbEntry.Tables.ContainsKey(tableName)) // minimize exception spam
#endif
                       dbEntry.Tables.Add(tableName, new TableEntry { });
                } catch (Exception) { }// can fail if duplicate added (we prefer not to lock)
            }
            return true;
        }

        public static List<string> GetColumns(NpgsqlConnection conn, string connectionString, string databaseName, string schema,  string tableName) {
            DBEntry dbEntry;
            if (!Databases.TryGetValue(databaseName, out dbEntry))
                dbEntry = GetDatabase(conn, connectionString);// we need to cache it now

            // check if we already have this table cached
            TableEntry tableEntry;
            if (!dbEntry.Tables.TryGetValue(tableName, out tableEntry)) {
                // we don't so add it to cache now
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
                // we don't have the columns yet, extract all column names
                // SELECT column_name FROM information_schema.columns WHERE table_schema = '..schema..' AND table_name = '..tablename..'
                using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                    List<string> cols = new List<string>();
                    cmd.Connection = conn;
                    cmd.CommandText = $"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{tableName}'";
                    using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
                        while (rdr.Read()) {
                            cols.Add(rdr.GetString(0));
                        }
                    }
                    if (cols.Count == 0) {
                        dbEntry.Tables.Remove(tableName);
                        throw new InternalError($"Request for db {databaseName} table {tableName} which doesn't exist");
                    }
                    tableEntry.Columns = cols;
                }
            }
            return tableEntry.Columns;
        }
    }
}
