/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Reflection;
using System.Text;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Extensions;
using YetaWF.Core.Language;
using YetaWF.Core.Models;
using YetaWF.Core.Models.Attributes;
using YetaWF.Core.Support;
using YetaWF.DataProvider.SQLGeneric;

namespace YetaWF.DataProvider.PostgreSQL {

    internal partial class SQLGen : SQLGenericGen {

        public NpgsqlConnection Conn { get; private set; }
        public int IdentitySeed { get; private set; }
        public bool Logging { get; private set; }
        public List<LanguageData> Languages { get; private set; }

        private class TableInfo {
            public Table CurrentTable { get; set; }
            public Table NewTable { get; set; }
            public List<TableInfo> SubTables { get; set; }
        }

        public SQLGen(NpgsqlConnection conn, List<LanguageData> languages, int identitySeed, bool logging) {
            Conn = conn;
            Languages = languages;
            IdentitySeed = identitySeed;
            Logging = logging;
        }

        public bool CreateTableFromModel(string dbName, string schema, string tableName, string key1Name, string key2Name, string identityName, List<PropertyData> propData, Type tpProps,
                List<string> errorList, 
                bool TopMost = false,
                bool SiteSpecific = false,
                string ForeignKeyTable = null,
                bool WithDerivedInfo = false,
                bool SubTable = false) {

            List<string> columns = new List<string>();
            TableInfo tableInfo = CreateSimpleTableFromModel(dbName, schema, tableName, key1Name, key2Name, identityName, propData, tpProps,
                errorList, columns,
                TopMost, SiteSpecific, ForeignKeyTable, WithDerivedInfo,
                SubTable);
            if (tableInfo == null)
                return false;

            MakeTables(dbName, schema, tableInfo, propData, tpProps, WithDerivedInfo);
            return true;
        }

        private TableInfo CreateSimpleTableFromModel(string dbName, string schema, string tableName, string key1Name, string key2Name, string identityName, List<PropertyData> propData, Type tpProps,
                List<string> errorList, List<string> columns,
                bool TopMost = false,
                bool SiteSpecific = false,
                string ForeignKeyTable = null,
                bool WithDerivedInfo = false,
                bool SubTable = false) {
            try {

                SQLManager sqlManager = new SQLManager();
                Table currentTable = null;
                Table newTable = new Table {
                    Name = tableName,
                };
                TableInfo tableInfo = new TableInfo {
                    NewTable = newTable,
                    CurrentTable = null,
                    SubTables = new List<TableInfo>(),
                };

                if (sqlManager.HasTable(Conn, dbName, schema, tableName)) {
                    currentTable = tableInfo.CurrentTable = new Table() {
                        Name = tableName,
                        Indexes = SQLManager.GetInfoIndexes(Conn, dbName, schema, tableName),
                        ForeignKeys = SQLManager.GetInfoForeignKeys(Conn, dbName, schema, tableName),
                        Columns = sqlManager.GetColumns(Conn, dbName, schema, tableName),
                    };
                }

                if (!SubTable) {
                    DropFunctionsAsync(dbName, schema, tableName);// drop functions so we can recreate types
                    DropType(dbName, schema, tableInfo.NewTable.Name, propData, tpProps, SubTable: false);// drop this type so we can recreate types for subtables
                }

                bool hasSubTable = AddTableColumns(dbName, schema, tableInfo, key1Name, key2Name, identityName, propData, tpProps, "", true, columns, errorList, SiteSpecific: SiteSpecific, WithDerivedInfo: WithDerivedInfo, SubTable: SubTable);

                // PK Index
                if (!SubTable) {
                    Index newIndex = new Index {
                        Name = "P" + tableName,
                    };
                    newIndex.IndexedColumns.Add(key1Name);
                    if (!string.IsNullOrWhiteSpace(key2Name))
                        newIndex.IndexedColumns.Add(key2Name);
                    if (SiteSpecific)
                        newIndex.IndexedColumns.Add(SQLBase.SiteColumn);
                    newIndex.IndexType = IndexType.PrimaryKey;
                    newTable.Indexes.Add(newIndex);
                }

                // Other indexes
                {
                    List<PropertyData> propIndexes = (
                        from p in propData
                        where p.HasAttribute(Data_IndexAttribute.AttributeName)
                        select p).ToList();
                    if (propIndexes != null) {
                        foreach (PropertyData propIndex in propIndexes) {
                            if (!HasIdentity(identityName) || propIndex.Name != identityName) { // identity columns are indexed later
                                if (propIndex.PropInfo.PropertyType == typeof(MultiString)) {
                                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                                    MultiString ms = new MultiString();
                                    foreach (LanguageData lang in Languages) {
                                        string col = SQLBase.ColumnFromPropertyWithLanguage(lang.Id, propIndex.Name);
                                        Index newIndex = new Index {
                                            Name = "K" + tableName + "_" + col,
                                        };
                                        newIndex.IndexedColumns.Add(col);
                                        newIndex.IndexType = IndexType.Indexed;
                                        newTable.Indexes.Add(newIndex);
                                    }
                                } else {
                                    Index newIndex = new Index {
                                        Name = "K" + tableName + "_" + propIndex.Name,
                                    };
                                    newIndex.IndexedColumns.Add(propIndex.Name);
                                    newIndex.IndexType = IndexType.Indexed;
                                    newTable.Indexes.Add(newIndex);
                                }
                            }
                        }
                    }
                    List<PropertyData> propUniques = (
                        from p in propData
                        where p.HasAttribute(Data_UniqueAttribute.AttributeName)
                        select p).ToList();
                    foreach (PropertyData propUnique in propUniques) {
                        Index newIndex = new Index {
                            Name = "U_" + tableName + "_" + propUnique.Name,
                        };
                        newIndex.IndexedColumns.Add(propUnique.Name);
                        newIndex.IndexType = IndexType.UniqueKey;
                        newTable.Indexes.Add(newIndex);
                    }
                }

                // Identity
                if (SubTable) { // for replication
                    Column newColumn = new Column {
                        Name = SQLBase.IdentityColumn,
                        DataType = NpgsqlDbType.Integer,
                        Nullable = false,
                        Identity = true,
                        IdentityIncrement = 1,
                        IdentitySeed = 1,
                    };
                    newTable.Columns.Add(newColumn);

                    Index newIndex = new Index {
                        Name = "K" + tableName + "_" + SQLBase.IdentityColumn,
                    };
                    newIndex.IndexedColumns.Add(SQLBase.IdentityColumn);
                    newIndex.IndexType = IndexType.UniqueKey;
                    newTable.Indexes.Add(newIndex);
                } else {
                    if (HasIdentity(identityName)) {
                        Column newColumn = new Column {
                            Name = identityName,
                            DataType = NpgsqlDbType.Integer,
                            Nullable = false,
                            Identity = true,
                            IdentityIncrement = 1,
                            IdentitySeed = IdentitySeed,
                        };
                        newTable.Columns.Add(newColumn);

                        Index newIndex = new Index {
                            Name = "K" + tableName + "_" + identityName,
                        };
                        newIndex.IndexedColumns.Add(identityName);
                        newIndex.IndexType = IndexType.UniqueKey;
                        newTable.Indexes.Add(newIndex);
                    } else {
                        if (hasSubTable) {
                            Column newColumn = new Column {
                                Name = SQLBase.IdentityColumn,
                                DataType = NpgsqlDbType.Integer,
                                Nullable = false,
                                Identity = true,
                                IdentityIncrement = 1,
                                IdentitySeed = 1,
                            };
                            newTable.Columns.Add(newColumn);

                            Index newIndex = new Index {
                                Name = "K" + tableName + "_" + SQLBase.IdentityColumn,
                            };
                            newIndex.IndexedColumns.Add(SQLBase.IdentityColumn);
                            newIndex.IndexType = IndexType.UniqueKey;
                            newTable.Indexes.Add(newIndex);
                        }
                    }
                }

                // Foreign keys
                if (ForeignKeyTable != null) {
                    if (SubTable) {
                        // a subtable uses the identity of the main table as key so we have to create the column as it's not part of the data

                        if (TopMost)
                            throw new InternalError("Topmost CreateTable call can't define a SubTable");
                        if (!HasIdentity(identityName))
                            throw new InternalError("Identity required");

                        Index newIndex = new Index {
                            Name = "K" + tableName + "_" + SQLBase.SubTableKeyColumn,
                        };
                        newIndex.IndexedColumns.Add(SQLBase.SubTableKeyColumn);
                        newIndex.IndexType = IndexType.Indexed;
                        newTable.Indexes.Add(newIndex);

                        // a subtable uses the identity of the main table as key
                        ForeignKey fk = new ForeignKey {
                            Name = "F" + tableName + SQLBase.SubTableKeyColumn + "_" + ForeignKeyTable + "_" + identityName
                        };
                        fk.ForeignKeyColumns.Add(new ForeignKeyColumn { Column = SQLBase.SubTableKeyColumn, ReferencedColumn = identityName });
                        fk.ReferencedTable = ForeignKeyTable;
                        newTable.ForeignKeys.Add(fk);
                    } else {
                        if (key2Name != null) throw new InternalError("Only a single key can be used with foreign keys");

                        ForeignKey fk = new ForeignKey {
                            Name = "F" + tableName + "_" + key1Name + "_" + ForeignKeyTable + "_" + key1Name
                        };
                        fk.ForeignKeyColumns.Add(new ForeignKeyColumn { Column = key1Name, ReferencedColumn = key1Name });
                        fk.ReferencedTable = ForeignKeyTable;
                        fk.ForeignKeyColumns.Add(new ForeignKeyColumn { Column = SQLBase.SiteColumn, ReferencedColumn = SQLBase.SiteColumn });
                        newTable.ForeignKeys.Add(fk);
                    }
                }

                return tableInfo;

            } catch (Exception exc) {
                if (Logging) YetaWF.Core.Log.Logging.AddErrorLog("Couldn't create table {0}", tableName, exc);
                errorList.Add(string.Format("Couldn't create table {0}", tableName));
                errorList.Add(ErrorHandling.FormatExceptionMessage(exc));
                return null;
            }
        }

        private bool AddTableColumns(string dbName, string schema, TableInfo tableInfo,
                string key1Name, string key2Name, string identityName,
                List<PropertyData> propData, Type tpContainer, string prefix, bool topMost, List<string> columns, List<string> errorList,
                bool SiteSpecific = false,
                bool WithDerivedInfo = false,
                bool SubTable = false) {

            Table newTable = tableInfo.NewTable;
            Table currentTable = tableInfo.CurrentTable;
            string result = ProcessColumns(
                (prefix, container, prop) => { // regular property
                    PropertyInfo pi = prop.PropInfo;
                    Column newColumn = new Column {
                        Name = $"{prefix}{prop.ColumnName}",
                        Nullable = true,
                    };
                    newColumn.DataType = GetDataType(pi);
                    if (pi.PropertyType == typeof(string)) {
                        StringLengthAttribute attr = (StringLengthAttribute)pi.GetCustomAttribute(typeof(StringLengthAttribute));
                        if (attr == null)
                            throw new InternalError($"StringLength attribute missing for property {prop.Name}");
                        int len = attr.MaximumLength;
                        if (len == 0 || len >= 4000)
                            newColumn.Length = 0;
                        else
                            newColumn.Length = len;
                    }
                    bool nullable = false;
                    if (prop.ColumnName != key1Name && prop.ColumnName != key2Name && (pi.PropertyType == typeof(string) || Nullable.GetUnderlyingType(pi.PropertyType) != null))
                        nullable = true;
                    newColumn.Nullable = nullable;
                    Data_NewValue newValAttr = (Data_NewValue)pi.GetCustomAttribute(typeof(Data_NewValue));
                    if (currentTable != null && !currentTable.HasColumn($"{prefix}{prop.ColumnName}")) {
                        if (newValAttr == null)
                            throw new InternalError($"Property {prop.Name} in table {newTable.Name} doesn't have a Data_NewValue attribute, which is required when updating tables");
                    }
                    newTable.Columns.Add(newColumn);
                    return null;
                },
                (prefix, container, prop) => { // Identity
                    return null;
                },
                (prefix, container, prop) => { // Binary
                    Column newColumn = new Column {
                        Name = $"{prefix}{prop.ColumnName}",
                        DataType = NpgsqlDbType.Bytea,
                        Nullable = true,
                    };
                    newTable.Columns.Add(newColumn);
                    return null;
                },
                (prefix, container, prop) => { // Image
                    Column newColumn = new Column {
                        Name = $"{prefix}{prop.ColumnName}",
                        DataType = NpgsqlDbType.Bytea,
                        Nullable = true,
                    };
                    newTable.Columns.Add(newColumn);
                    return null;
                },
                (prefix, container, prop) => { // MultiString
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sb = new StringBuilder();
                    foreach (LanguageData lang in Languages) {
                        string colName = SQLBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name);
                        Column newColumn = new Column {
                            Name = $"{prefix}{colName}",
                            DataType = NpgsqlDbType.Varchar,
                        };
                        StringLengthAttribute attr = (StringLengthAttribute)prop.PropInfo.GetCustomAttribute(typeof(StringLengthAttribute));
                        if (attr == null)
                            throw new InternalError("StringLength attribute missing for property {0}", prefix + prop.Name);
                        if (attr.MaximumLength >= 4000)
                            newColumn.Length = 0;
                        else
                            newColumn.Length = attr.MaximumLength;
                        if (prop.Name != key1Name && prop.Name != key2Name)
                            newColumn.Nullable = true;
                        newTable.Columns.Add(newColumn);
                    }
                    return sb.ToString();
                },
                (prefix, container, name) => { // Predefined property
                    if (name == "DerivedTableName")
                        newTable.Columns.Add(new Column {
                            Name = $"{prefix}{name}",
                            DataType = NpgsqlDbType.Varchar,
                            Length = 80,
                        });
                    else if (name == "DerivedDataType")
                        newTable.Columns.Add(new Column {
                            Name = $"{prefix}{name}",
                            DataType = NpgsqlDbType.Varchar,
                            Length = 200,
                        });
                    else if (name == "DerivedAssemblyName")
                        newTable.Columns.Add(new Column {
                            Name = $"{prefix}{name}",
                            DataType = NpgsqlDbType.Varchar,
                            Length = 200,
                        });
                    else if (name == SQLGenericBase.SubTableKeyColumn)
                        newTable.Columns.Add(new Column {
                            Name = $"{prefix}{SQLGenericBase.SubTableKeyColumn}",
                            DataType = NpgsqlDbType.Integer,
                        });
                    else if (name == SQLGenericBase.SiteColumn)
                        newTable.Columns.Add(new Column {
                            Name = $"{prefix}{SQLGenericBase.SiteColumn}",
                            DataType = NpgsqlDbType.Integer,
                        });

                    return null;
                },
                (prefix, container, prop, subPropData, subType, subTableName) => { // Subtable property
                    // create a table that links the main table and this enumerated type using the key of the table
                    TableInfo subTableInfo = CreateSimpleTableFromModel(dbName, schema, subTableName, SQLBase.SubTableKeyColumn, null,
                        HasIdentity(identityName) ? identityName : SQLBase.IdentityColumn, subPropData, subType, errorList, columns,
                        TopMost: false,
                        ForeignKeyTable: newTable.Name,
                        SubTable: true,
                        SiteSpecific: false);
                    if (subTableInfo == null)
                        throw new InternalError($"Creation of subtable {subTableName} failed");
                    tableInfo.SubTables.Add(subTableInfo);

                    return "subtable";
                },
                dbName, schema, newTable.Name, null, propData, tpContainer, columns, prefix, topMost, SiteSpecific, WithDerivedInfo, SubTable);

            return !string.IsNullOrEmpty(result);
        }

        private void MakeTables(string dbName, string schema, TableInfo tableInfo, List<PropertyData> propData, Type tpProps, bool WithDerivedInfo = false) {
            if (tableInfo.CurrentTable != null) {
                RemoveIndexesAndForeignKeys(dbName, schema, tableInfo);
            }
            MakeTable(dbName, schema, tableInfo, propData, tpProps, WithDerivedInfo);
        }

        private void MakeTable(string dbName, string schema, TableInfo tableInfo, List<PropertyData> propData, Type tpProps, bool WithDerivedInfo = false) {
            if (tableInfo.CurrentTable != null)
                UpdateTable(dbName, schema, tableInfo.CurrentTable, tableInfo.NewTable);
            else
                CreateTable(dbName, schema, tableInfo.NewTable);
            foreach (TableInfo subtableInfo in tableInfo.SubTables) {
                if (subtableInfo.CurrentTable != null)
                    UpdateTable(dbName, schema, subtableInfo.CurrentTable, subtableInfo.NewTable);
                else
                    CreateTable(dbName, schema, subtableInfo.NewTable);
            }
        }

        internal void MakeTypes(string dbName, string schema, string tableName, List<PropertyData> propData, Type tpProps) {

            List<SubTableInfo> subTables = GetSubTables(tableName, propData);

            // Update cached info for new table and subtables
            SQLManager sqlManager = new SQLManager();
            SQLGenericManagerCache.ClearCache();
            sqlManager.GetColumns(Conn, dbName, schema, tableName);
            foreach (SubTableInfo subtable in subTables) {
                sqlManager.GetColumns(Conn, dbName, schema, subtable.Name);
            }

            // Make all types for subtables and table
            foreach (SubTableInfo subtable in subTables) {
                List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subtable.Type);
                MakeType(dbName, schema, subtable.Name, subPropData, subtable.Type, SubTable: true);
            }

            MakeType(dbName, schema, tableName, propData, tpProps, SubTable: false);
        }

        internal void MakeTypesWithBaseType(string dbName, string schema, string baseDataset, string dataset, List<PropertyData> combinedProps, List<PropertyData> basePropData, List<PropertyData> propData, Type baseType, Type type) {

            // Update cached info for new table and subtables
            SQLManager sqlManager = new SQLManager();
            SQLGenericManagerCache.ClearCache();
            List<SubTableInfo> subTables = GetSubTables(dataset, propData);
            sqlManager.GetColumns(Conn, dbName, schema, dataset);
            foreach (SubTableInfo subtable in subTables) {
                sqlManager.GetColumns(Conn, dbName, schema, subtable.Name);
            }

            //MakeType(dbName, schema, dataset, combinedProps, type, WithDerivedInfo: true);
            //MakeType(dbName, schema, dataset, combinedProps, type, WithDerivedInfo: false);

            MakeTypeWithBaseType(dbName, schema, baseDataset, dataset, combinedProps, basePropData, propData, baseType, type);
        }

        private void CreateTable(string dbName, string schema, Table newTable) {

            StringBuilder sb = new StringBuilder();
            StringBuilder sbIx = new StringBuilder();

            sb.Append($@"
CREATE TABLE ""{schema}"".""{newTable.Name}"" (");

            // Columns
            foreach (Column col in newTable.Columns) {
                sb.Append($@"
""{col.Name}"" {GetDataTypeString(col)}{GetIdentity(col)}{GetNullable(col)},");
            }

            // Indexes

            foreach (Index index in newTable.Indexes) {
                switch (index.IndexType) {
                    case IndexType.PrimaryKey:
                    case IndexType.UniqueKey:
                        sb.Append(GetAddIndex(index, dbName, schema, newTable));
                        break;
                    case IndexType.Indexed:
                        // created separately, after table
                        sbIx.Append(GetAddIndex(index, dbName, schema, newTable));
                        break;
                }
            }

            // Foreign Keys

            if (newTable.ForeignKeys.Count > 0) {
                foreach (ForeignKey fk in newTable.ForeignKeys) {
                    sb.Append(GetAddForeignKey(fk, dbName, schema, newTable));
                }
            }
            sb.RemoveLastComma();

            sb.Append($@"
) WITH ( OIDS = FALSE );

");

            sb.Append(sbIx.ToString());

            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = Conn;
                cmd.CommandText = sb.ToString();
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }

        private void RemoveIndexesAndForeignKeys(string dbName, string schema, TableInfo tableInfo) {

            foreach (TableInfo t in tableInfo.SubTables) {
                RemoveIndexesAndForeignKeys(dbName, schema, t);
            }

            Table currentTable = tableInfo.CurrentTable;
            Table newTable = tableInfo.NewTable;

            List<Index> removedIndexes;
            List<ForeignKey> removedForeignKeys;

            removedIndexes = currentTable.Indexes.Except(newTable.Indexes, new IndexComparer()).ToList();
            removedForeignKeys = currentTable.ForeignKeys.Except(newTable.ForeignKeys, new ForeignKeyComparer()).ToList();

            StringBuilder sb = new StringBuilder();

            // Remove index
            foreach (Index index in removedIndexes) {
                switch (index.IndexType) {
                    case IndexType.Indexed:
                        sb.Append($@"
DROP INDEX {index.Name};");
                        break;
                    case IndexType.UniqueKey:
                        sb.Append($@"
ALTER TABLE ""{schema}"".""{newTable.Name}"" DROP CONSTRAINT ""{index.Name}"";");
                        break;
                    case IndexType.PrimaryKey:
                        sb.Append($@"
ALTER TABLE ""{schema}"".""{newTable.Name}"" DROP CONSTRAINT ""{index.Name}"";");
                        break;
                }
            }

            // Remove foreign key
            foreach (ForeignKey fk in removedForeignKeys) {
                sb.Append($@"
ALTER TABLE ""{schema}"".""{newTable.Name}"" DROP CONSTRAINT ""{fk.Name}"";");
            }

            if (sb.Length != 0) {
                using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                    cmd.Connection = Conn;
                    cmd.CommandText = sb.ToString();
                    YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void UpdateTable(string dbName, string schema, Table currentTable, Table newTable) {

            StringBuilder sb = new StringBuilder();

            List<Column> removedColumns = currentTable.Columns.Except(newTable.Columns, new ColumnComparer()).ToList();
            List<Column> addedColumns = newTable.Columns.Except(currentTable.Columns, new ColumnComparer()).ToList();
            List<Column> alteredColumns = addedColumns.Intersect(removedColumns, new ColumnNameComparer()).ToList();
            removedColumns = removedColumns.Except(alteredColumns, new ColumnNameComparer()).ToList();
            addedColumns = addedColumns.Except(alteredColumns, new ColumnNameComparer()).ToList();

            List<Index> addedIndexes = newTable.Indexes.Except(currentTable.Indexes, new IndexComparer()).ToList();

            // Remove columns
            foreach (Column col in removedColumns) {
                sb.Append($@"
IF EXISTS (SELECT * FROM  [""{schema}""].[sysobjects] WHERE id = OBJECT_ID(N'DF_{currentTable.Name}_{col.Name}') AND type = 'D')
    BEGIN
       ALTER TABLE  [""{schema}""].[{newTable.Name}] DROP CONSTRAINT [DF_{currentTable.Name}_{col.Name}], COLUMN [{col.Name}];
    END
ELSE
    BEGIN
       ALTER TABLE  [""{schema}""].[{newTable.Name}] DROP COLUMN [{col.Name}];
    END
");
            }

            // Add columns
            foreach (Column col in addedColumns) {
                sb.Append($@"
ALTER TABLE [""{schema}""].[{newTable.Name}] ADD [{col.Name}] {GetDataTypeString(col)}{GetDataTypeDefault(newTable.Name, col)}{GetIdentity(col)}{GetNullable(col)};");
            }
            // Altered columns
            foreach (Column col in alteredColumns) {
                sb.Append($@"
ALTER TABLE [""{schema}""].[{newTable.Name}] ALTER [{col.Name}] {GetDataTypeString(col)}{GetDataTypeDefault(newTable.Name, col)}{GetIdentity(col)}{GetNullable(col)};");
            }

            // Add index
            foreach (Index index in addedIndexes) {
                switch (index.IndexType) {
                    case IndexType.PrimaryKey:
                    case IndexType.UniqueKey:
                        sb.Append($@"
ALTER TABLE ""{schema}"".""{newTable.Name}""
    ADD {GetAddIndex(index, dbName, schema, newTable)}");
                        sb.RemoveLastComma();
                        sb.Append($@"
;");
                        break;
                    case IndexType.Indexed:
                        sb.Append($@"
{GetAddIndex(index, dbName, schema, newTable)}");
                        break;
                }
            }

            if (sb.Length != 0) {
                using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                    cmd.Connection = Conn;
                    cmd.CommandText = sb.ToString();
                    YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void UpdateForeignKeys(string dbName, string schema, Table currentTable, Table newTable) {

            StringBuilder sb = new StringBuilder();

            List<ForeignKey> addedForeignKeys = newTable.ForeignKeys.Except(currentTable.ForeignKeys, new ForeignKeyComparer()).ToList();
            foreach (ForeignKey fk in addedForeignKeys) {
                sb.Append(GetAddForeignKey(fk, dbName, schema, newTable));
            }

            if (sb.Length != 0) {
                using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                    cmd.Connection = Conn;
                    cmd.CommandText = sb.ToString();
                    YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public static NpgsqlDbType GetDataType(PropertyInfo pi) {
            Type tp = pi.PropertyType;
            if (tp == typeof(DateTime) || tp == typeof(DateTime?))
                return NpgsqlDbType.Timestamp;
            else if (tp == typeof(long) || tp == typeof(long?))
                return NpgsqlDbType.Bigint;
            else if (tp == typeof(TimeSpan) || tp == typeof(TimeSpan?))
                return NpgsqlDbType.Interval;
            else if (tp == typeof(decimal) || tp == typeof(decimal?))
                return NpgsqlDbType.Money;
            else if (tp == typeof(bool) || tp == typeof(bool?))
                return NpgsqlDbType.Bit;
            else if (tp == typeof(System.Guid) || tp == typeof(System.Guid?))
                return NpgsqlDbType.Uuid;
            else if (tp == typeof(Image))
                return NpgsqlDbType.Bytea;
            else if (tp == typeof(int) || tp == typeof(int?))
                return NpgsqlDbType.Integer;
            else if (tp == typeof(Single) || tp == typeof(Single?))
                return NpgsqlDbType.Double;
            else if (tp.IsEnum)
                return NpgsqlDbType.Integer;
            else if (tp == typeof(string))
                return NpgsqlDbType.Varchar;
            throw new InternalError("Unsupported property type {0} for property {1}", tp.FullName, pi.Name);
        }
        private string GetDataTypeString(Column col) {
            switch (col.DataType) {
                case NpgsqlDbType.Bigint:
                    return "bigint";
                case NpgsqlDbType.Interval:
                    return "interval";
                case NpgsqlDbType.Bit:
                    return "boolean";
                case NpgsqlDbType.Timestamp:
                    return "timestamp without time zone";
                case NpgsqlDbType.Money:
                    return "money";
                case NpgsqlDbType.Uuid:
                    return "uuid";
                case NpgsqlDbType.Bytea:
                    return "bytea";
                case NpgsqlDbType.Integer:
                    return "integer";
                case NpgsqlDbType.Double:
                    return "double precision";
                case NpgsqlDbType.Varchar:
                    if (col.Length == 0)
                        return "text";
                    else
                        return $"character varying({col.Length})";
                default:
                    throw new InternalError($"Column {col.Name} has unsupported type name {col.DataType.ToString()}");
            }
        }
        internal string GetDataTypeArgumentString(Column col) {
            switch (col.DataType) {
                case NpgsqlDbType.Bigint:
                    return "bigint";
                case NpgsqlDbType.Interval:
                    return "interval";
                case NpgsqlDbType.Bit:
                    return "boolean";
                case NpgsqlDbType.Timestamp:
                    return "timestamp without time zone";
                case NpgsqlDbType.Money:
                    return "money";
                case NpgsqlDbType.Uuid:
                    return "uuid";
                case NpgsqlDbType.Bytea:
                    return "bytea";
                case NpgsqlDbType.Integer:
                    return "integer";
                case NpgsqlDbType.Double:
                    return "double precision";
                case NpgsqlDbType.Varchar:
                    if (col.Length == 0)
                        return "text";
                    else
                        return $"character varying({col.Length})";
                default:
                    throw new InternalError($"Column {col.Name} has unsupported type name {col.DataType.ToString()}");
            }
        }
        private string GetDataTypeDefault(string tableName, Column col) {
            if (col.Nullable)
                return "";

            switch (col.DataType) {
                case NpgsqlDbType.Bigint:
                    return $" SET DEFAULT 0";
                case NpgsqlDbType.Interval:
                    return $"";
                case NpgsqlDbType.Bit:
                    return $" SET DEFAULT 0";
                case NpgsqlDbType.Timestamp:
                    return "";
                case NpgsqlDbType.Money:
                    return $" SET DEFAULT 0";
                case NpgsqlDbType.Uuid:
                    return "";
                case NpgsqlDbType.Bytea:
                    return "";
                case NpgsqlDbType.Integer:
                    return $" SET DEFAULT 0";
                case NpgsqlDbType.Double:
                    return $" SET DEFAULT 0";
                case NpgsqlDbType.Varchar:
                    return "";
                default:
                    throw new InternalError($"Table {tableName}, column {col.Name} has an unsupported type name {col.DataType.ToString()}");
            }
        }
        private string GetNullable(Column col) {
            if (col.Nullable) {
                return " NULL";
            } else {
                return " NOT NULL";
            }
        }
        private string GetIdentity(Column col) {
            if (col.Identity) {
                return $" GENERATED ALWAYS AS IDENTITY(START WITH {col.IdentitySeed} INCREMENT BY {col.IdentityIncrement})";
            }
            return "";
        }
        private string GetAddIndex(Index index, string dbName, string schema, Table newTable) {
            StringBuilder sb = new StringBuilder();
            sb.Append($"");
            switch (index.IndexType) {
                case IndexType.PrimaryKey:
                    sb.Append($@"
    CONSTRAINT ""{index.Name}"" PRIMARY KEY (");
                    foreach (string col in index.IndexedColumns) {
                        sb.Append($@"""{col}"",");
                    }
                    sb.RemoveLastComma();
                    sb.Append($"),");
                    break;
                case IndexType.UniqueKey:
                    sb.Append($@"  
    CONSTRAINT ""{index.Name}"" UNIQUE (");
                    foreach (string col in index.IndexedColumns) {
                        sb.Append($@"""{col}"",");
                    }
                    sb.RemoveLastComma();
                    sb.Append($"),");
                    break;
                case IndexType.Indexed:
                    // created separately, after table
                    sb.Append($@"
    CREATE INDEX ""{index.Name}"" ON ""{schema}"".""{newTable.Name}"" USING btree (");
                    foreach (string col in index.IndexedColumns) {
                        sb.Append($@"""{col}"",");
                    }
                    sb.RemoveLastComma();
                    sb.Append($@")
;");
                    break;
            }
            return sb.ToString();
        }
        private string GetAddForeignKey(ForeignKey fk, string dbName, string schema, Table newTable) {
            StringBuilder sb = new StringBuilder();
            sb.Append($@"
    CONSTRAINT ""{fk.Name}"" FOREIGN KEY (");
            foreach (ForeignKeyColumn fkCol in fk.ForeignKeyColumns) {
                sb.Append($@"""{fkCol.Column}"",");
            }
            sb.RemoveLastComma();
            sb.Append($@")");
            sb.Append($@"
    REFERENCES ""{schema}"".""{fk.ReferencedTable}"" (");
            foreach (ForeignKeyColumn fkCol in fk.ForeignKeyColumns) {
                sb.Append($@"""{fkCol.ReferencedColumn}"",");
            }
            sb.RemoveLastComma();
            sb.Append($@") MATCH SIMPLE");

            sb.Append($@"
    ON UPDATE NO ACTION
    ON DELETE CASCADE,");

            return sb.ToString();
        }

        private void MakeType(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpType, bool SubTable = false, bool WithDerivedInfo = false) {

            SQLBuilder sb = new SQLBuilder();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            if (propData.Count <= 1)
                return;

            string typeName = $"{dataset}_T";

            sb.Append($@"
DROP TYPE IF EXISTS ""{schema}"".""{typeName}"";
CREATE TYPE ""{schema}"".""{typeName}"" AS
(");
            sb.Append($@"
{GetTypeNameList(dbName, schema, dataset, propData, tpType, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: WithDerivedInfo, SubTable: SubTable)}");

            sb.RemoveLastComma();

            sb.Append($@"
);");

            // Add to database
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = Conn;
                cmd.CommandText = sb.ToString();
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }

        private void MakeTypeWithBaseType(string dbName, string schema, string baseDataset, string dataset, List<PropertyData> combinedProps, List<PropertyData> basePropData, List<PropertyData> propData, Type baseType, Type type) {

            SQLBuilder sb = new SQLBuilder();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            if (combinedProps.Count <= 1)
                return;

            string typeName = $"{dataset}_T";

            List<PropertyData> propDataNoDups = combinedProps.Except(basePropData, new PropertyDataComparer()).ToList();

            sb.Append($@"
DROP TYPE IF EXISTS ""{schema}"".""{typeName}"";
CREATE TYPE ""{schema}"".""{typeName}"" AS
(");
            sb.Append($@"
{GetTypeNameList(dbName, schema, baseDataset, basePropData, baseType, Prefix: null, TopMost: false, SiteSpecific: true, WithDerivedInfo: false, SubTable: false)}
{GetTypeNameList(dbName, schema, dataset, propDataNoDups, type, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: false)}");
            sb.RemoveLastComma();

            sb.Append($@"
);");

#if NOTWANTED            
            sb.Append($@"
DROP TYPE IF EXISTS ""{schema}"".""{typeName}_DRV"";
CREATE TYPE ""{schema}"".""{typeName}_DRV"" AS
(");
            sb.Append($@"
{GetTypeNameList(dbName, schema, baseDataset, basePropData, baseType, Prefix: null, TopMost: false, SiteSpecific: true, WithDerivedInfo: true, SubTable: false)}
{GetTypeNameList(dbName, schema, dataset, propDataNoDups, type, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: false)}");
            sb.RemoveLastComma();

            sb.Append($@"
);
");
#endif

            // Add to database
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = Conn;
                cmd.CommandText = sb.ToString();
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
        }

        private void DropType(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpType, bool SubTable = false) {

            SQLBuilder sb = new SQLBuilder();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string typeName = $"{dataset}_T";

            sb.Append($@"
DROP TYPE IF EXISTS ""{schema}"".""{typeName}"";");

            // Add to database
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = Conn;
                cmd.CommandText = sb.ToString();
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }

        }
    }
}