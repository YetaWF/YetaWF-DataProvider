﻿/* Copyright © 2019 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.SQL {

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
    internal static class SQLManager {

        public class Database {
            public string DataSource { get; set; }
            public string Name { get; set; }
            public Database() {
                CachedTables = new Dictionary<string, Table>();
            }
            public Dictionary<string, Table> CachedTables { get; set; }
        }
        public class Table {
            public string Name { get; set; }
            public Table() {
                CachedColumns = new List<string>();
            }
            public List<string> CachedColumns { get; set; }
        }

        private static Dictionary<string, Database> Databases = new Dictionary<string, Database>();

        internal static Database GetDatabase(SqlConnection conn, string dbName) {
            Database db = GetDatabaseCond(conn, dbName);
            if (db == null)
                throw new InternalError("Can't connect to SQL database {0}", conn.Database);
            return db;
        }

        internal static Database GetDatabaseCond(SqlConnection conn, string dbName) {
            Database db = (from d in Databases.Values where conn.DataSource == conn.DataSource && dbName == d.Name select d).FirstOrDefault();
            if (db == null) {
                using (SqlCommand cmd = new SqlCommand()) {
                    cmd.Connection = conn;
                    cmd.CommandText = "SELECT name FROM master.sys.databases";
                    using (SqlDataReader rdr = cmd.ExecuteReader()) {
                        while (rdr.Read()) {
                            string name = rdr.GetString(0);
                            if (!Databases.ContainsKey(name)) {
                                try {
                                    Databases.Add(name, new Database {
                                        Name = name,
                                        DataSource = conn.DataSource,
                                    });
                                } catch (Exception) { }// can fail if duplicate added (we prefer not to lock)
                            }
                        }
                    }
                    db = (from d in Databases.Values where conn.DataSource == conn.DataSource && dbName == d.Name select d).FirstOrDefault();
                    return db;
                }
            }
            return db;
        }

        internal static bool HasTable(SqlConnection conn, string databaseName, string dbo, string tableName) {
            return GetTable(conn, databaseName, dbo, tableName) != null;
        }
        internal static Table GetTable(SqlConnection conn, string databaseName, string dbo, string tableName) {
            List<Table> tables = GetTables(conn, databaseName, dbo);
            if (tables == null)
                return null;
            return (from t in tables where t.Name == tableName select t).FirstOrDefault();
        }
        internal static List<Table> GetTables(SqlConnection conn, string databaseName, string dbo) {
            Database db = GetDatabaseCond(conn, databaseName);
            if (db == null)
                return null;
            if (db.CachedTables.Count == 0) {
                using (SqlCommand cmd = new SqlCommand()) {
                    Dictionary<string, Table> tables = new Dictionary<string, Table>();
                    cmd.Connection = conn;
                    cmd.CommandText = $"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{dbo}'";
                    using (SqlDataReader rdr = cmd.ExecuteReader()) {
                        while (rdr.Read()) {
                            string name = rdr.GetString(0);
                            tables.Add(name, new Table {
                                Name = name,
                            });
                        }
                    }
                    db.CachedTables = tables;
                }
            }
            return db.CachedTables.Values.ToList();
        }

        internal static List<string> GetColumns(SqlConnection conn, string databaseName, string dbo, string tableName) {
            List<string> columns = GetColumnsCond(conn, databaseName, dbo, tableName);
            if (columns == null)
                throw new InternalError($"Request for SQL DB {databaseName} table {tableName} which doesn't exist", databaseName, tableName);
            return columns;
        }
        internal static List<string> GetColumnsCond(SqlConnection conn, string databaseName, string dbo, string tableName) {
            Table table = GetTable(conn, databaseName, dbo, tableName);
            if (table == null)
                return null;
            if (table.CachedColumns.Count == 0) {
                using (SqlCommand cmd = new SqlCommand()) {
                    List<string> columns = new List<string>();
                    cmd.Connection = conn;
                    cmd.CommandText = $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{tableName}'";
                    using (SqlDataReader rdr = cmd.ExecuteReader()) {
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

        internal static List<SQLGen.Column> GetInfoColumns(SqlConnection conn, string name, string dbOwner, string tableName) {

            List<SQLGen.Column> list = new List<SQLGen.Column>();
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{tableName}'";
                using (SqlDataReader rdr = cmd.ExecuteReader()) {
                    while (rdr.Read()) {
                        SQLGen.Column col = new SQLGen.Column {
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
                foreach (SQLGen.Column column in list) {
                    cmd.CommandText = $@"Select is_identity from sys.columns WHERE Object_Name([object_id]) = '{tableName}' AND [name] = '{column.Name}'";
                    object o = cmd.ExecuteScalar();
                    if (o != null && !(o is System.DBNull))
                        column.Identity = Convert.ToInt32(o) > 0 ? true : false;
                }
                return list;
            }
        }
        internal static List<SQLGen.Index> GetInfoIndexes(SqlConnection conn, string name, string dbOwner, string tableName) {

            List<SQLGen.Index> list = new List<SQLGen.Index>();
            using (SqlCommand cmd = new SqlCommand()) {
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
                using (SqlDataReader rdr = cmd.ExecuteReader()) {
                    while (rdr.Read()) {
                        SQLGen.Index newIndex = new SQLGen.Index {
                            Name = rdr.GetString(0),
                        };
                        bool isPrimary = rdr.GetBoolean(1);
                        bool isUnique = rdr.GetBoolean(2);

                        if (isPrimary)
                            newIndex.IndexType = SQLGen.IndexType.PrimaryKey;
                        else if (isUnique)
                            newIndex.IndexType = SQLGen.IndexType.UniqueKey;
                        else
                            newIndex.IndexType = SQLGen.IndexType.Indexed;
                        list.Add(newIndex);
                    }
                }
                return list;
            }
        }

        internal static List<SQLGen.ForeignKey> GetInfoForeignKeys(SqlConnection conn, string name, string dbOwner, string tableName) {

            List<SQLGen.ForeignKey> list = new List<SQLGen.ForeignKey>();
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'FOREIGN KEY' AND TABLE_NAME = N'{tableName}'";
                using (SqlDataReader rdr = cmd.ExecuteReader()) {
                    while (rdr.Read()) {
                        list.Add(new SQLGen.ForeignKey {
                            Name = rdr.GetString(0),
                        });
                    }
                }
            }
            return list;
        }

        private static SqlDbType GetDataType(string typeName) {
            if (typeName == "datetime2")
                return SqlDbType.DateTime2;
            else if (typeName == "bigint")
                return SqlDbType.BigInt;
            else if (typeName == "money")
                return SqlDbType.Money;
            else if (typeName == "bit")
                return SqlDbType.Bit;
            else if (typeName == "uniqueidentifier")
                return SqlDbType.UniqueIdentifier;
            else if (typeName == "varbinary")
                return SqlDbType.VarBinary;
            else if (typeName == "int")
                return SqlDbType.Int;
            else if (typeName == "float")
                return SqlDbType.Float;
            else if (typeName == "nvarchar")
                return SqlDbType.NVarChar;
            throw new InternalError($"Unsupported type name {typeName}");
        }


        internal static void DropForeignKey(SqlConnection conn, string databaseName, string dbo, string tableName, string foreignKey) {
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"ALTER TABLE [{dbo}].[{tableName}] DROP CONSTRAINT [{foreignKey}]";
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropIndex(SqlConnection conn, string databaseName, string dbo, string tableName, string index) {
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"DROP INDEX {SQLBuilder.WrapBrackets(dbo)}.{SQLBuilder.WrapBrackets(tableName)}.{index}";
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropUniqueKeyIndex(SqlConnection conn, string databaseName, string dbo, string tableName, string index) {
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $@"ALTER TABLE [{dbo}].[{tableName}] DROP CONSTRAINT [{index}]";
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropPrimaryKeyIndex(SqlConnection conn, string databaseName, string dbo, string tableName, string index) {
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"ALTER TABLE [{dbo}].[{tableName}] DROP CONSTRAINT [{index}]";
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropTable(SqlConnection conn, string databaseName, string dbo, string tableName) {
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"DROP TABLE {SQLBuilder.WrapBrackets(databaseName)}.{SQLBuilder.WrapBrackets(dbo)}.{SQLBuilder.WrapBrackets(tableName)}";
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Clear the cache.
        /// </summary>
        internal static void ClearCache() {
            Databases = Databases = new Dictionary<string, Database>();
        }
    }
}
