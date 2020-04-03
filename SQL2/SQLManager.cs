/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.Data;
using YetaWF.Core.Support;
using YetaWF.DataProvider.SQLGeneric;
#if MVC6
using Microsoft.Data.SqlClient;
#else
using System.Data.SqlClient;
#endif

namespace YetaWF.DataProvider.SQL2 {

    /// <summary>
    /// Used to retrieve information about databases, tables, columns.
    /// </summary>
    /// <remarks>To minimize startup time, we collect table names and column names as they are accessed the first time (lazy loading).
    ///
    /// Some of this could be made async, but often this is called in a context that is not async.
    /// As this is all cached and most of it is only used during model install/uninstall, using non-async is just easier.
    ///
    /// The SQL queries used for model install/uninstall (like GetColumnNames) are not very efficient, but cached. Simplicity over perfection.
    /// </remarks>
    internal class SQLManager : SQLGenericManager<SqlConnection> {

        public override string GetDataSource(SqlConnection connInfo) {
            return connInfo.DataSource;
        }
        public override List<string> GetDataBaseNames(SqlConnection connInfo) {
            List<string> list = new List<string>();
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = connInfo;
                cmd.CommandText = "SELECT name FROM master.sys.databases";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                using (SqlDataReader rdr = cmd.ExecuteReader()) {
                    while (rdr.Read()) {
                        string name = rdr.GetString(0);
                        list.Add(name);
                    }
                }
            }
            return list;
        }
        public override List<SQLGenericGen.Table> GetTableNames(SqlConnection connInfo, string databaseName, string schemaNotUsed) {
            List<SQLGenericGen.Table> list = new List<SQLGenericGen.Table>();
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = connInfo;
                cmd.CommandText = $"SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                using (SqlDataReader rdr = cmd.ExecuteReader()) {
                    while (rdr.Read()) {
                        string name = rdr.GetString(0);
                        string schema = rdr.GetString(1);
                        list.Add(new SQLGenericGen.Table {
                            Schema = schema,
                            Name = name,
                        });
                    }
                }
            }
            return list;
        }

        public override List<SQLGenericGen.Column> GetColumnNames(SqlConnection connInfo, string name, string dbOwner, string tableName) {

            List<SQLGenericGen.Column> list = new List<SQLGenericGen.Column>();
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = connInfo;
                cmd.CommandText = $"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{tableName}'";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                using (SqlDataReader rdr = cmd.ExecuteReader()) {
                    while (rdr.Read()) {
                        SQLGenericGen.Column col = new SQLGenericGen.Column {
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
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                using (SqlDataReader rdr = cmd.ExecuteReader()) {
                    while (rdr.Read()) {
                        string ixName = rdr.GetString(0);
                        if (!ixName.StartsWith(DBFIRST_PREFIX)) {
                            SQLGen.Index newIndex = new SQLGen.Index {
                                Name = ixName,
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
                }
                return list;
            }
        }

        internal static List<SQLGen.ForeignKey> GetInfoForeignKeys(SqlConnection conn, string name, string dbOwner, string tableName) {

            List<SQLGen.ForeignKey> list = new List<SQLGen.ForeignKey>();
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'FOREIGN KEY' AND TABLE_NAME = N'{tableName}'";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                using (SqlDataReader rdr = cmd.ExecuteReader()) {
                    while (rdr.Read()) {
                        string fkName = rdr.GetString(0);
                        if (!fkName.StartsWith(DBFIRST_PREFIX)) {
                            list.Add(new SQLGen.ForeignKey {
                                Name = fkName,
                            });
                        }
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
            else if (typeName == "date")
                return SqlDbType.DateTime2;
            throw new InternalError($"Unsupported type name {typeName}");
        }


        internal static void DropForeignKey(SqlConnection conn, string databaseName, string dbo, string tableName, string foreignKey) {
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"ALTER TABLE [{dbo}].[{tableName}] DROP CONSTRAINT [{foreignKey}]";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropIndex(SqlConnection conn, string databaseName, string dbo, string tableName, string index) {
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"DROP INDEX {SQLBuilder.WrapIdentifier(dbo)}.{SQLBuilder.WrapIdentifier(tableName)}.{index}";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropUniqueKeyIndex(SqlConnection conn, string databaseName, string dbo, string tableName, string index) {
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $@"ALTER TABLE [{dbo}].[{tableName}] DROP CONSTRAINT [{index}]";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropPrimaryKeyIndex(SqlConnection conn, string databaseName, string dbo, string tableName, string index) {
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"ALTER TABLE [{dbo}].[{tableName}] DROP CONSTRAINT [{index}]";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropTable(SqlConnection conn, string databaseName, string dbo, string tableName) {
            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"DROP TABLE {SQLBuilder.WrapIdentifier(databaseName)}.{SQLBuilder.WrapIdentifier(dbo)}.{SQLBuilder.WrapIdentifier(tableName)}";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }
    }
}
