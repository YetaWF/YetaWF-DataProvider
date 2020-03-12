/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System.Collections.Generic;
using System.Data;
using System.Linq;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.SQLGeneric {

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

        // Cached database information
        private static List<SQLGenericGen.Database> Databases = new List<SQLGenericGen.Database>();
        private static readonly object DatabasesLockObject = new object();

        public abstract string GetDataSource(TYPE connInfo);
        public abstract List<string> GetDataBaseNames(TYPE connInfo);
        public abstract List<SQLGenericGen.Table> GetTableNames(TYPE connInfo, string databaseName, string schema);
        public abstract List<SQLGenericGen.Column> GetColumnNames(TYPE connInfo, string databaseName, string schema, string tableName);

        public SQLGenericGen.Database GetDatabase(TYPE connInfo, string dbName){
            SQLGenericGen.Database db = GetDatabaseCond(connInfo, dbName);
            if (db == null)
                throw new InternalError("Can't connect to database {0}", dbName);
            return db;
        }
        public SQLGenericGen.Database GetDatabaseCond(TYPE connInfo, string dbName) { 
            dbName = dbName.ToLower();
            string connDataSource = GetDataSource(connInfo);
            string connDataSourceLow = connDataSource.ToLower();
            SQLGenericGen.Database db;
            lock (DatabasesLockObject) {
                db = (from d in Databases where d.DataSource.ToLower() == connDataSourceLow && dbName == d.Name.ToLower() select d).FirstOrDefault();
                if (db == null) {
                    List<string> dbNames = GetDataBaseNames(connInfo);
                    foreach (string dbn in dbNames) {
                        SQLGenericGen.Database d = (from s in Databases where s.DataSource.ToLower() == connDataSourceLow && dbn == s.Name.ToLower() select s).FirstOrDefault();
                        if (d == null) {
                            Databases.Add(new SQLGenericGen.Database {
                                Name = dbn,
                                DataSource = connDataSource,
                            });
                        }
                    }
                    db = (from d in Databases where d.DataSource.ToLower() == connDataSourceLow && dbName == d.Name.ToLower() select d).FirstOrDefault();
                }
            }
            return db;
        }

        public bool HasTable(TYPE connInfo, string databaseName, string schema, string tableName) {
            return GetTable(connInfo, databaseName, schema, tableName) != null;
        }
        public SQLGenericGen.Table GetTable(TYPE connInfo, string databaseName, string schema, string tableName) {
            List<SQLGenericGen.Table> tables = GetTables(connInfo, databaseName, schema);
            if (tables == null)
                return null;
            tableName = tableName.ToLower();
            schema = schema.ToLower();
            return (from t in tables where t.Name.ToLower() == tableName && t.Schema.ToLower() == schema select t).FirstOrDefault();
        }
        public List<SQLGenericGen.Table> GetTables(TYPE connInfo, string databaseName, string schema) {
            SQLGenericGen.Database db = GetDatabaseCond(connInfo, databaseName);
            if (db == null)
                return null;
            if (db.CachedTables.Count == 0)
                db.CachedTables = GetTableNames(connInfo, databaseName, schema);
            schema = schema.ToLower();
            return (from t in db.CachedTables where t.Schema.ToLower() == schema select t).ToList();
        }

        public List<string> GetColumnsOnly(TYPE connInfo, string databaseName, string schema, string tableName) {
            return (from c in GetColumns(connInfo, databaseName, schema, tableName) select c.Name).ToList();
        }
        public List<SQLGenericGen.Column> GetColumns(TYPE connInfo, string databaseName, string schema, string tableName) {
            List<SQLGenericGen.Column> columns = GetColumnsCond(connInfo, databaseName, schema, tableName);
            if (columns == null)
                throw new InternalError($"Request for DB {databaseName} table {tableName} which doesn't exist", databaseName, tableName);
            return columns;
        }
        public List<SQLGenericGen.Column> GetColumnsCond(TYPE connInfo, string databaseName, string schema, string tableName) {
            SQLGenericGen.Table table = GetTable(connInfo, databaseName, schema, tableName);
            if (table == null)
                return null;
            if (table.CachedColumns.Count == 0)
                table.CachedColumns = GetColumnNames(connInfo, databaseName, schema, tableName);
            return table.CachedColumns;
        }

        /// <summary>
        /// Clear the cache.
        /// </summary>
        public void ClearCache() {
            lock (DatabasesLockObject) {
                Databases = new List<SQLGenericGen.Database>();
            }
        }
    }
}
