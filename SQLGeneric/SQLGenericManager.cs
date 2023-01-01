/* Copyright © 2023 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System.Collections.Generic;
using System.Data;
using System.Linq;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.SQLGeneric {

    /// <summary>
    /// Implements a cache for all accessed databases, tables and columns.
    /// </summary>
    public static class SQLGenericManagerCache {

        // Cached database information
        internal static List<SQLGenericGen.Database> Databases = new List<SQLGenericGen.Database>();
        internal static readonly object DatabasesLockObject = new object();

        /// <summary>
        /// Clear the cache.
        /// </summary>
        public static void ClearCache() {
            lock (SQLGenericManagerCache.DatabasesLockObject) {
                SQLGenericManagerCache.Databases = new List<SQLGenericGen.Database>();
            }
        }
    }

    /// <summary>
    /// Used to retrieve information about databases, tables, columns.
    /// </summary>
    /// <remarks>To minimize startup time, we collect table names and column names as they are accessed the first time (lazy loading).
    ///
    /// Some of this could be made async, but often this is called in a context that is not async.
    /// As this is all cached and most of it is only used during model install/uninstall, using non-async is just easier.
    /// The SQL queries used for model install/uninstall (like GetColumnInfo) are not very efficient. Simplicity over perfection.
    /// </remarks>
    public abstract class SQLGenericManager<TYPE> {

        /// <summary>
        /// When using a "Database First" approach, any index or foreign key whose name starts with this prefix will be ignored and remain untouched/unaltered by model updates.
        /// </summary>
        public const string DBFIRST_PREFIX = "PREDEF_";

        /// <summary>
        /// Returns the data source. The contents are dependent on the SQL dataprovider used.
        /// </summary>
        /// <param name="connInfo">The SQL-specific connection information.</param>
        /// <returns>Returns the SQL-specific data source name.</returns>
        public abstract string GetDataSource(TYPE connInfo);

        /// <summary>
        /// Returns the database names given the SQL-specific connection information. The contents are dependent on the SQL dataprovider used.
        /// </summary>
        /// <param name="connInfo">The SQL-specific connection information.</param>
        /// <returns>Returns the SQL-specific the database names.</returns>
        public abstract List<string> GetDataBaseNames(TYPE connInfo);

        /// <summary>
        /// Returns the table names given the SQL-specific connection information and database name. The contents are dependent on the SQL dataprovider used.
        /// </summary>
        /// <param name="connInfo">The SQL-specific connection information.</param>
        /// <param name="databaseName">The database name.</param>
        /// <param name="schema">The SQL-specific schema.</param>
        /// <returns>Returns the SQL-specific the table names.</returns>
        public abstract List<SQLGenericGen.Table> GetTableNames(TYPE connInfo, string databaseName, string schema);

        /// <summary>
        /// Returns the column names given the SQL-specific connection information, database and table name. The contents are dependent on the SQL dataprovider used.
        /// </summary>
        /// <param name="connInfo">The SQL-specific connection information.</param>
        /// <param name="databaseName">The database name.</param>
        /// <param name="schema">The SQL-specific schema.</param>
        /// <param name="tableName">The SQL-specific table name.</param>
        /// <returns>Returns the SQL-specific the table names.</returns>
        public abstract List<SQLGenericGen.Column> GetColumnNames(TYPE connInfo, string databaseName, string schema, string tableName);

        /// <summary>
        /// Returns database information given a database name.
        /// </summary>
        /// <param name="connInfo">The SQL-specific connection information.</param>
        /// <param name="dbName">The database name.</param>
        /// <returns>Returns database information.</returns>
        /// <remarks>If the database does not exists, an exception occurs.</remarks>
        public SQLGenericGen.Database GetDatabase(TYPE connInfo, string dbName){
            SQLGenericGen.Database? db = GetDatabaseCond(connInfo, dbName);
            if (db == null)
                throw new InternalError("Can't connect to database {0}", dbName);
            return db;
        }
        /// <summary>
        /// Returns database information given a database name.
        /// </summary>
        /// <param name="connInfo">The SQL-specific connection information.</param>
        /// <param name="dbName">The database name.</param>
        /// <returns>Returns database information if the database exists, null otherwise.</returns>
        public SQLGenericGen.Database? GetDatabaseCond(TYPE connInfo, string dbName) {
            dbName = dbName.ToLower();
            string connDataSource = GetDataSource(connInfo);
            string connDataSourceLow = connDataSource.ToLower();
            SQLGenericGen.Database? db;
            lock (SQLGenericManagerCache.DatabasesLockObject) {
                db = (from d in SQLGenericManagerCache.Databases where d.DataSource.ToLower() == connDataSourceLow && dbName == d.Name.ToLower() select d).FirstOrDefault();
                if (db == null) {
                    List<string> dbNames = GetDataBaseNames(connInfo);
                    foreach (string dbn in dbNames) {
                        SQLGenericGen.Database? d = (from s in SQLGenericManagerCache.Databases where s.DataSource.ToLower() == connDataSourceLow && dbn == s.Name.ToLower() select s).FirstOrDefault();
                        if (d == null) {
                            SQLGenericManagerCache.Databases.Add(new SQLGenericGen.Database {
                                Name = dbn,
                                DataSource = connDataSource,
                            });
                        }
                    }
                    db = (from d in SQLGenericManagerCache.Databases where d.DataSource.ToLower() == connDataSourceLow && dbName == d.Name.ToLower() select d).FirstOrDefault();
                }
            }
            return db;
        }

        /// <summary>
        /// Returns whether the specified table exists.
        /// </summary>
        /// <param name="connInfo">The SQL-specific connection information.</param>
        /// <param name="databaseName">The database name.</param>
        /// <param name="schema">The schema.</param>
        /// <param name="tableName">The table name.</param>
        /// <returns>Returns true if the table exists, false otherwise.</returns>
        public bool HasTable(TYPE connInfo, string databaseName, string schema, string tableName) {
            return GetTable(connInfo, databaseName, schema, tableName) != null;
        }
        /// <summary>
        /// Returns table information for the specified table.
        /// </summary>
        /// <param name="connInfo">The SQL-specific connection information.</param>
        /// <param name="databaseName">The database name.</param>
        /// <param name="schema">The schema.</param>
        /// <param name="tableName">The table name.</param>
        /// <returns>Returns table information if the table exists, null otherwise.</returns>
        public SQLGenericGen.Table? GetTable(TYPE connInfo, string databaseName, string schema, string tableName) {
            List<SQLGenericGen.Table>? tables = GetTables(connInfo, databaseName, schema);
            if (tables == null)
                return null;
            tableName = tableName.ToLower();
            schema = schema.ToLower();
            return (from t in tables where t.Name.ToLower() == tableName && t.Schema.ToLower() == schema select t).FirstOrDefault();
        }
        /// <summary>
        /// Returns table information for all tables in the specified database.
        /// </summary>
        /// <param name="connInfo">The SQL-specific connection information.</param>
        /// <param name="databaseName">The database name.</param>
        /// <param name="schema">The schema.</param>
        /// <returns>Returns table information for all tables. If the database doesn't exist, null is returned.</returns>
        public List<SQLGenericGen.Table>? GetTables(TYPE connInfo, string databaseName, string schema) {
            SQLGenericGen.Database? db = GetDatabaseCond(connInfo, databaseName);
            if (db == null)
                return null;
            if (db.CachedTables.Count == 0)
                db.CachedTables = GetTableNames(connInfo, databaseName, schema);
            schema = schema.ToLower();
            return (from t in db.CachedTables where t.Schema.ToLower() == schema select t).ToList();
        }

        /// <summary>
        /// Returns column information for all columns in the specified database table.
        /// </summary>
        /// <param name="connInfo">The SQL-specific connection information.</param>
        /// <param name="databaseName">The database name.</param>
        /// <param name="schema">The schema.</param>
        /// <param name="tableName">The table name.</param>
        /// <returns>Returns column information for all columns.</returns>
        /// <remarks>If the database table doesn't exist, an exception occurs.</remarks>
        public List<SQLGenericGen.Column> GetColumns(TYPE connInfo, string databaseName, string schema, string tableName) {
            List<SQLGenericGen.Column>? columns = GetColumnsCond(connInfo, databaseName, schema, tableName);
            if (columns == null)
                throw new InternalError($"Request for DB {databaseName} table {tableName} which doesn't exist", databaseName, tableName);
            return columns;
        }
        /// <summary>
        /// Returns column information for all columns in the specified database table.
        /// </summary>
        /// <param name="connInfo">The SQL-specific connection information.</param>
        /// <param name="databaseName">The database name.</param>
        /// <param name="schema">The schema.</param>
        /// <param name="tableName">The table name.</param>
        /// <returns>Returns column information for all columns. If the database table doesn't exist, null is returned.</returns>
        /// <remarks>If the database table doesn't exist, an exception occurs.</remarks>
        public List<SQLGenericGen.Column>? GetColumnsCond(TYPE connInfo, string databaseName, string schema, string tableName) {
            SQLGenericGen.Table? table = GetTable(connInfo, databaseName, schema, tableName);
            if (table == null)
                return null;
            if (table.CachedColumns.Count == 0)
                table.CachedColumns = GetColumnNames(connInfo, databaseName, schema, tableName);
            return table.CachedColumns;
        }

        /// <summary>
        /// Returns column information for the specified column.
        /// </summary>
        /// <param name="connInfo">The SQL-specific connection information.</param>
        /// <param name="databaseName">The database name.</param>
        /// <param name="schema">The schema.</param>
        /// <param name="tableName">The table name.</param>
        /// <param name="columnName">The column name.</param>
        /// <returns>Returns column information.</returns>
        /// <remarks>If the column doesn't exist, an exception occurs.</remarks>
        public SQLGenericGen.Column GetColumn(TYPE connInfo, string databaseName, string schema, string tableName, string columnName) {
            List<SQLGenericGen.Column> columns = GetColumns(connInfo, databaseName, schema, tableName);
            SQLGenericGen.Column? col = (from c in columns where string.Compare(c.Name, columnName, true) == 0 select c).FirstOrDefault();
            if (col == null)
                throw new InternalError($"Expected column {columnName} not found in {databaseName}, {schema}, {tableName}");
            return col;
        }
    }
}
