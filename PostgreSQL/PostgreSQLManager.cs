/* Copyright © 2019 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.PostgreSQL {

    /// <summary>
    /// Used to retrieve information about databases, tables, columns.
    /// </summary>
    /// <remarks>To minimize startup time, we collect table names and column names as they are accessed the first time (lazy loading).
    ///
    /// Some of this could be made async, but often this is called in a context that is not async.
    /// As this is all cached and most of it is only used during model install/uninstall, using non-async is just easier.
    ///
    /// The SQL queries used for model install/uninstall (like GetColumnInfo) is not very efficient. Simplicity over perfection.
    /// </remarks>
    internal static class PostgreSQLManager {
        /// <summary>
        /// When using a "Database First" approach, any index or foreign key whose name starts with this prefix will be ignored and remain untouched/unaltered by model updates.
        /// </summary>
        public const string DBFIRST_PREFIX = "PREDEF_";

        public class Database {
            public string DataSource { get; set; }
            public string Name { get; set; }
            public Database() {
                CachedTables = new List<Table>();
            }
            public List<Table> CachedTables { get; set; }
        }
        public class Table {
            public string Name { get; set; }
            public Table() {
                CachedColumns = new List<string>();
            }
            public List<string> CachedColumns { get; set; }
        }

        private static List<Database> Databases = new List<Database>();

        internal static Database GetDatabase(NpgsqlConnection conn, string dbName) {
            Database db = GetDatabaseCond(conn, dbName);
            if (db == null)
                throw new InternalError("Can't connect to SQL database {0}", conn.Database);
            return db;
        }

        internal static Database GetDatabaseCond(NpgsqlConnection conn, string dbName) {
            dbName = dbName.ToLower();
            string connLow = conn.DataSource.ToLower();
            Database db = (from d in Databases where d.DataSource.ToLower() == connLow && dbName == d.Name.ToLower() select d).FirstOrDefault();
            if (db == null) {
                using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                    cmd.Connection = conn;
                    cmd.CommandText = "SELECT datname FROM pg_database WHERE NOT datistemplate";
                    using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
                        while (rdr.Read()) {
                            string name = rdr.GetString(0);
                            Database d = (from s in Databases where s.DataSource.ToLower() == connLow && dbName == s.Name.ToLower() select s).FirstOrDefault();
                            if (d == null) {
                                Databases.Add(new Database {
                                    Name = name,
                                    DataSource = conn.DataSource,
                                });
                            }
                        }
                    }
                    db = (from d in Databases where d.DataSource.ToLower() == connLow && dbName == d.Name.ToLower() select d).FirstOrDefault();
                }
            }
            return db;
        }

        internal static bool HasTable(NpgsqlConnection conn, string databaseName, string schema, string tableName) {
            return GetTable(conn, databaseName, schema, tableName) != null;
        }
        internal static Table GetTable(NpgsqlConnection conn, string databaseName, string schema, string tableName) {
            List<Table> tables = GetTables(conn, databaseName, schema);
            if (tables == null)
                return null;
            tableName = tableName.ToLower();
            return (from t in tables where t.Name.ToLower() == tableName select t).FirstOrDefault();
        }
        internal static List<Table> GetTables(NpgsqlConnection conn, string databaseName, string schema) {
            Database db = GetDatabaseCond(conn, databaseName);
            if (db == null)
                return null;
            if (db.CachedTables.Count == 0) {
                using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                    List<Table> tables = new List<Table>();
                    cmd.Connection = conn;
                    cmd.CommandText = $"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'";
                    using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
                        while (rdr.Read()) {
                            string name = rdr.GetString(0);
                            tables.Add(new Table {
                                Name = name,
                            });
                        }
                    }
                    db.CachedTables = tables;
                }
            }
            return db.CachedTables.ToList();
        }

        internal static List<string> GetColumns(NpgsqlConnection conn, string databaseName, string schema, string tableName) {
            List<string> columns = GetColumnsCond(conn, databaseName, schema, tableName);
            if (columns == null)
                throw new InternalError($"Request for SQL DB {databaseName} table {tableName} which doesn't exist", databaseName, tableName);
            return columns;
        }
        internal static List<string> GetColumnsCond(NpgsqlConnection conn, string databaseName, string schema, string tableName) {
            Table table = GetTable(conn, databaseName, schema, tableName);
            if (table == null)
                return null;
            if (table.CachedColumns.Count == 0) {
                using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                    List<string> columns = new List<string>();
                    cmd.Connection = conn;
                    cmd.CommandText = $"SELECT column_name FROM information_schema.columns WHERE table_name = '{tableName}'";
                    using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
                        while (rdr.Read()) {
                            string name = rdr.GetString(0);
                            columns.Add(name);
                        }
                    }
                    table.CachedColumns = columns;
                }
            }
            return table.CachedColumns;
        }

        internal static List<PostgreSQLGen.Column> GetInfoColumns(NpgsqlConnection conn, string name, string schema, string tableName) {

            List<PostgreSQLGen.Column> list = new List<PostgreSQLGen.Column>();
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"SELECT column_name, data_type, is_nullable, character_maximum_length FROM information_schema.columns WHERE table_name = N'{tableName}'";
                using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
                    while (rdr.Read()) {
                        PostgreSQLGen.Column col = new PostgreSQLGen.Column {
                            Name = rdr.GetString(0),
                            DataType = GetDataType(rdr.GetString(1)),
                            Nullable = rdr.GetString(2) == "YES",
                        };
                        object oLength = rdr.GetValue(3);
                        if (oLength != null && !(oLength is System.DBNull))
                            col.Length = Math.Max(0, Convert.ToInt32(oLength));
                        list.Add(col);
                    }
                }
                // Identity for all columns
                foreach (PostgreSQLGen.Column column in list) {
                    cmd.CommandText = $@"Select is_identity from sys.columns WHERE Object_Name([object_id]) = '{tableName}' AND [name] = '{column.Name}'";
                    object o = cmd.ExecuteScalar();
                    if (o != null && !(o is System.DBNull))
                        column.Identity = Convert.ToInt32(o) > 0 ? true : false;
                }
                return list;
            }
        }
        internal static List<PostgreSQLGen.Index> GetInfoIndexes(NpgsqlConnection conn, string name, string schema, string tableName) {

            List<PostgreSQLGen.Index> list = new List<PostgreSQLGen.Index>();
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $@"
    SELECT i.name AS index_name
        -- ,i.type_desc
        -- ,is_unique
        -- ,ds.type_desc AS filegroup_or_partition_scheme
        -- ,ds.name AS filegroup_or_partition_scheme_name
        -- ,ignore_dup_key
        ,is_primary_key
        ,is_unique_constraint
        -- ,fill_factor
        -- ,is_padded
        -- ,is_disabled
        -- ,allow_row_locks
        -- ,allow_page_locks
    FROM sys.indexes AS i
	INNER JOIN sys.data_spaces AS ds ON i.data_space_id = ds.data_space_id
    WHERE is_hypothetical = 0 AND i.index_id<> 0 AND i.object_id = OBJECT_ID('{tableName}');";
                using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
                    while (rdr.Read()) {
                        string ixName = rdr.GetString(0);
                        if (!ixName.StartsWith(DBFIRST_PREFIX)) {
                            PostgreSQLGen.Index newIndex = new PostgreSQLGen.Index {
                                Name = ixName,
                            };
                            bool isPrimary = rdr.GetBoolean(1);
                            bool isUnique = rdr.GetBoolean(2);

                            if (isPrimary)
                                newIndex.IndexType = PostgreSQLGen.IndexType.PrimaryKey;
                            else if (isUnique)
                                newIndex.IndexType = PostgreSQLGen.IndexType.UniqueKey;
                            else
                                newIndex.IndexType = PostgreSQLGen.IndexType.Indexed;
                            list.Add(newIndex);
                        }
                    }
                }
                return list;
            }
        }

        internal static List<PostgreSQLGen.ForeignKey> GetInfoForeignKeys(NpgsqlConnection conn, string name, string schema, string tableName) {

            List<PostgreSQLGen.ForeignKey> list = new List<PostgreSQLGen.ForeignKey>();
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_type = 'FOREIGN KEY' AND table_name = '{tableName}'";
                using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
                    while (rdr.Read()) {
                        string fkName = rdr.GetString(0);
                        if (!fkName.StartsWith(DBFIRST_PREFIX)) {
                            list.Add(new PostgreSQLGen.ForeignKey {
                                Name = fkName,
                            });
                        }
                    }
                }
            }
            return list;
        }

        private static SqlDbType GetDataType(string typeName) { //$$$$ FIX THIS
            if (typeName == "date")
                return SqlDbType.DateTime2;
            else if (typeName == "bigint")
                return SqlDbType.BigInt;
            else if (typeName == "money")//$$$??
                return SqlDbType.Money;
            else if (typeName == "boolean")
                return SqlDbType.Bit;
            else if (typeName == "uuid")
                return SqlDbType.UniqueIdentifier;
            else if (typeName == "bytea")
                return SqlDbType.VarBinary;
            else if (typeName == "integer")
                return SqlDbType.Int;
            else if (typeName == "float")//$$$$
                return SqlDbType.Float;
            else if (typeName == "character varying")
                return SqlDbType.NVarChar;
            else if (typeName == "text")
                return SqlDbType.NVarChar;
            throw new InternalError($"Unsupported type name {typeName}");
        }


        internal static void DropForeignKey(NpgsqlConnection conn, string databaseName, string dbo, string tableName, string foreignKey) {
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"ALTER TABLE [{dbo}].[{tableName}] DROP CONSTRAINT [{foreignKey}]";
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropIndex(NpgsqlConnection conn, string databaseName, string dbo, string tableName, string index) {
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"DROP INDEX {PostgreSQLBuilder.WrapQuotes(dbo)}.{PostgreSQLBuilder.WrapQuotes(tableName)}.{index}";
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropUniqueKeyIndex(NpgsqlConnection conn, string databaseName, string dbo, string tableName, string index) {
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $@"ALTER TABLE [{dbo}].[{tableName}] DROP CONSTRAINT [{index}]";
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropPrimaryKeyIndex(NpgsqlConnection conn, string databaseName, string dbo, string tableName, string index) {
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"ALTER TABLE [{dbo}].[{tableName}] DROP CONSTRAINT [{index}]";
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropTable(NpgsqlConnection conn, string databaseName, string dbo, string tableName) {
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"DROP TABLE {PostgreSQLBuilder.WrapQuotes(databaseName)}.{PostgreSQLBuilder.WrapQuotes(dbo)}.{PostgreSQLBuilder.WrapQuotes(tableName)}";
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Clear the cache.
        /// </summary>
        internal static void ClearCache() {
            Databases = new List<Database>();
        }
    }
}
