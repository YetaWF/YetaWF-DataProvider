/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using YetaWF.Core.Support;
using YetaWF.DataProvider.SQLGeneric;

namespace YetaWF.DataProvider.PostgreSQL {

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
    internal class SQLManager : SQLGenericManager<NpgsqlConnection> {

        public override string GetDataSource(NpgsqlConnection connInfo) {
            return connInfo.DataSource;
        }
        public override List<string> GetDataBaseNames(NpgsqlConnection connInfo) {
            List<string> list = new List<string>();
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = connInfo;
                cmd.CommandText = "SELECT datname FROM pg_database WHERE NOT datistemplate";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
                    while (rdr.Read()) {
                        string name = rdr.GetString(0);
                        list.Add(name);
                    }
                }
            }
            return list;
        }
        public override List<SQLGenericGen.Table> GetTableNames(NpgsqlConnection connInfo, string databaseName, string schemaNotUsed) {
            List<SQLGenericGen.Table> list = new List<SQLGenericGen.Table>();
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = connInfo;
                cmd.CommandText = $"SELECT table_name, table_schema FROM information_schema.tables"; // WHERE table_schema = '{schema}'";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
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

        public override List<SQLGenericGen.Column> GetColumnNames(NpgsqlConnection connInfo, string name, string schema, string tableName) {

            List<SQLGenericGen.Column> list = new List<SQLGenericGen.Column>();
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = connInfo;
                cmd.CommandText = $"SELECT column_name, data_type, is_nullable, character_maximum_length FROM information_schema.columns WHERE table_name = N'{tableName}'";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
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
                foreach (SQLGenericGen.Column column in list) {
                    cmd.CommandText = $@"Select is_identity from information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{tableName}' AND column_name = '{column.Name}'";
                    object? o = cmd.ExecuteScalar();
                    if (o != null && !(o is System.DBNull))
                        column.Identity = (string)(o) == "YES";
                }
                return list;
            }
        }

        internal static List<SQLGen.Index> GetInfoIndexes(NpgsqlConnection conn, string name, string schema, string tableName) {

            List<SQLGen.Index> list = new List<SQLGen.Index>();
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $@"
 SELECT
    i.relname AS indexname,
    x.indisprimary as indisprimary,
    x.indisunique as indisunique,
    -- *,
	n.nspname AS schemaname,
    c.relname AS tablename,
    t.spcname AS tablespace,
    pg_get_indexdef(i.oid) AS indexdef
   FROM pg_index x
     JOIN pg_class c ON c.oid = x.indrelid
     JOIN pg_class i ON i.oid = x.indexrelid
     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace
  WHERE (c.relkind = ANY (ARRAY['r'::""char"", 'm'::""char""])) AND i.relkind = 'i'::""char"" AND
     c.relname = '{tableName}'";

                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
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

        internal static List<SQLGen.ForeignKey> GetInfoForeignKeys(NpgsqlConnection conn, string name, string schema, string tableName) {

            List<SQLGen.ForeignKey> list = new List<SQLGen.ForeignKey>();
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $"SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_type = 'FOREIGN KEY' AND table_name = '{tableName}'";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                using (NpgsqlDataReader rdr = cmd.ExecuteReader()) {
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

        private static NpgsqlDbType GetDataType(string typeName) {
            if (typeName == "timestamp without time zone")
                return NpgsqlDbType.Timestamp;
            else if (typeName == "bigint")
                return NpgsqlDbType.Bigint;
            else if (typeName == "interval")
                return NpgsqlDbType.Interval;
            else if (typeName == "money")
                return NpgsqlDbType.Money;
            else if (typeName == "boolean")
                return NpgsqlDbType.Bit;
            else if (typeName == "uuid")
                return NpgsqlDbType.Uuid;
            else if (typeName == "bytea")
                return NpgsqlDbType.Bytea;
            else if (typeName == "integer")
                return NpgsqlDbType.Integer;
            else if (typeName == "double precision")//$$$$
                return NpgsqlDbType.Double;
            else if (typeName == "character varying")
                return NpgsqlDbType.Varchar;
            else if (typeName == "text")
                return NpgsqlDbType.Varchar;
            throw new InternalError($"Unsupported type name {typeName}");
        }


        internal static void DropForeignKey(NpgsqlConnection conn, string databaseName, string schema, string tableName, string foreignKey) {
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $@"ALTER TABLE ""{schema}"".""{tableName}"" DROP CONSTRAINT ""{foreignKey}""";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropIndex(NpgsqlConnection conn, string databaseName, string schema, string tableName, string index) {
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $@"DROP INDEX ""{schema}"".""{tableName}"".""{index}""";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropUniqueKeyIndex(NpgsqlConnection conn, string databaseName, string schema, string tableName, string index) {
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $@"ALTER TABLE ""{schema}"".""{tableName}"" DROP CONSTRAINT ""{index}""";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropPrimaryKeyIndex(NpgsqlConnection conn, string databaseName, string schema, string tableName, string index) {
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $@"ALTER TABLE ""{schema}"".""{tableName}"" DROP CONSTRAINT ""{index}""";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }
        internal static void DropTable(NpgsqlConnection conn, string databaseName, string schema, string tableName) {
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = conn;
                cmd.CommandText = $@"DROP TABLE ""{databaseName}"".""{schema}"".""{tableName}""";
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }
    }
}
