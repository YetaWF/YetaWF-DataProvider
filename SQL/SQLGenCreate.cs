/* Copyright © 2019 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Reflection;
using System.Text;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Language;
using YetaWF.Core.Models;
using YetaWF.Core.Models.Attributes;
using YetaWF.Core.Support;
using YetaWF.DataProvider.SQL;

namespace YetaWF.DataProvider {

    internal partial class SQLGen {

        // This isn't used any longer.
        // It was used to drop all constraints, foreign keys, but that turned out to be a problem.
        // We don't really need it right now. Dropping all indexes/foreign keys was used
        // when columns had to be converted from varchar to nvarchar. If such an issue comes up again,
        // we have to handle it in UpdateTable.
        public bool MajorDataChange = false;

        public SqlConnection Conn { get; private set; }
        public int IdentitySeed { get; private set; }
        public bool Logging { get; private set; }
        public List<LanguageData> Languages { get; private set; }

        private class TableInfo {
            public Table CurrentTable { get; set; }
            public Table NewTable { get; set; }
            public List<TableInfo> SubTables { get; set; }
        }

        public SQLGen(SqlConnection conn, List<LanguageData> languages, int identitySeed, bool logging) {
            Conn = conn;
            Languages = languages;
            IdentitySeed = identitySeed;
            Logging = logging;
        }

        protected bool HasIdentity(string identityName) {
            return !string.IsNullOrWhiteSpace(identityName);
        }

        public bool CreateTableFromModel(string dbName, string dbOwner, string tableName, string key1Name, string key2Name, string identityName, List<PropertyData> propData, Type tpProps,
                List<string> errorList, List<string> columns,
                bool TopMost = false,
                bool SiteSpecific = false,
                string ForeignKeyTable = null,
                string DerivedDataTableName = null, string DerivedDataTypeName = null, string DerivedAssemblyName = null,
                bool SubTable = false) {

            TableInfo tableInfo = CreateSimpleTableFromModel(dbName, dbOwner, tableName, key1Name, key2Name, identityName, propData, tpProps,
                errorList, columns,
                TopMost, SiteSpecific, ForeignKeyTable, DerivedDataTableName, DerivedDataTypeName, DerivedAssemblyName,
                SubTable);
            if (tableInfo == null)
                return false;

            MakeTables(dbName, dbOwner, tableInfo);

            return true;
        }

        private TableInfo CreateSimpleTableFromModel(string dbName, string dbOwner, string tableName, string key1Name, string key2Name, string identityName, List<PropertyData> propData, Type tpProps,
                List<string> errorList, List<string> columns,
                bool TopMost = false,
                bool SiteSpecific = false,
                string ForeignKeyTable = null,
                string DerivedDataTableName = null, string DerivedDataTypeName = null, string DerivedAssemblyName = null,
                bool SubTable = false) {
            try {

                Table currentTable = null;
                Table newTable = new Table {
                    Name = tableName,
                };
                TableInfo tableInfo = new TableInfo {
                    NewTable = newTable,
                    CurrentTable = null,
                    SubTables = new List<TableInfo>(),
                };

                if (SQLManager.HasTable(Conn, dbName, dbOwner, tableName)) {
                    currentTable = tableInfo.CurrentTable = new Table() {
                        Name = tableName,
                        Indexes = SQLManager.GetInfoIndexes(Conn, dbName, dbOwner, tableName),
                        ForeignKeys = SQLManager.GetInfoForeignKeys(Conn, dbName, dbOwner, tableName),
                        Columns = SQLManager.GetInfoColumns(Conn, dbName, dbOwner, tableName),
                    };
                }
                bool hasSubTable = AddTableColumns(dbName, dbOwner, tableInfo, key1Name, key2Name, identityName, propData, tpProps, "", true, columns, errorList, SubTable: SubTable);

                // if this table (base class) has a derived type, add its table name and its derived type as a column
                if (DerivedDataTableName != null) {
                    newTable.Columns.Add(new Column {
                        Name = DerivedDataTableName,
                        DataType = SqlDbType.NVarChar,
                        Length = 80,
                    });

                    newTable.Columns.Add(new Column {
                        Name = DerivedDataTypeName,
                        DataType = SqlDbType.NVarChar,
                        Length = 200,
                    });

                    newTable.Columns.Add(new Column {
                        Name = DerivedAssemblyName,
                        DataType = SqlDbType.NVarChar,
                        Length = 200,
                    });
                }
                if (SiteSpecific) {
                    newTable.Columns.Add(new Column {
                        Name = SQLBase.SiteColumn,
                        DataType = SqlDbType.Int,
                    });
                }

                // PK Index
                if (!SubTable) {
                    Index newIndex = new Index {
                        Name = "PK_" + tableName,
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
                    foreach (PropertyData propIndex in propIndexes) {
                        if (!HasIdentity(identityName) || propIndex.Name != identityName) { // identity columns are indexed later
                            if (propIndex.PropInfo.PropertyType == typeof(MultiString)) {
                                if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                                MultiString ms = new MultiString();
                                foreach (var lang in Languages) {
                                    string col = SQLBase.ColumnFromPropertyWithLanguage(lang.Id, propIndex.Name);
                                    Index newIndex = new Index {
                                        Name = "K_" + tableName + "_" + col,
                                    };
                                    newIndex.IndexedColumns.Add(col);
                                    newIndex.IndexType = IndexType.Indexed;
                                    newTable.Indexes.Add(newIndex);
                                }
                            } else {
                                Index newIndex = new Index {
                                    Name = "K_" + tableName + "_" + propIndex.Name,
                                };
                                newIndex.IndexedColumns.Add(propIndex.Name);
                                newIndex.IndexType = IndexType.Indexed;
                                newTable.Indexes.Add(newIndex);
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
                        DataType = SqlDbType.Int,
                        Nullable = false,
                        Identity = true,
                        IdentityIncrement = 1,
                        IdentitySeed = 1,
                    };
                    newTable.Columns.Add(newColumn);

                    Index newIndex = new Index {
                        Name = "K_" + tableName + "_" + SQLBase.IdentityColumn,
                    };
                    newIndex.IndexedColumns.Add(SQLBase.IdentityColumn);
                    newIndex.IndexType = IndexType.UniqueKey;
                    newTable.Indexes.Add(newIndex);
                } else {
                    if (HasIdentity(identityName)) {
                        Column newColumn = new Column {
                            Name = identityName,
                            DataType = SqlDbType.Int,
                            Nullable = false,
                            Identity = true,
                            IdentityIncrement = 1,
                            IdentitySeed = IdentitySeed,
                        };
                        newTable.Columns.Add(newColumn);

                        Index newIndex = new Index {
                            Name = "K_" + tableName + "_" + identityName,
                        };
                        newIndex.IndexedColumns.Add(identityName);
                        newIndex.IndexType = IndexType.UniqueKey;
                        newTable.Indexes.Add(newIndex);
                    } else {
                        if (hasSubTable) {
                            Column newColumn = new Column {
                                Name = SQLBase.IdentityColumn,
                                DataType = SqlDbType.Int,
                                Nullable = false,
                                Identity = true,
                                IdentityIncrement = 1,
                                IdentitySeed = 1,
                            };
                            newTable.Columns.Add(newColumn);

                            Index newIndex = new Index {
                                Name = "K_" + tableName + "_" + SQLBase.IdentityColumn,
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

                        Column newColumn = new Column {
                            Name = SQLBase.SubTableKeyColumn,
                            DataType = SqlDbType.Int,
                            Nullable = false,
                        };
                        newTable.Columns.Add(newColumn);

                        Index newIndex = new Index {
                            Name = "K_" + tableName + "_" + SQLBase.SubTableKeyColumn,
                        };
                        newIndex.IndexedColumns.Add(SQLBase.SubTableKeyColumn);
                        newIndex.IndexType = IndexType.Indexed;
                        newTable.Indexes.Add(newIndex);

                        // a subtable uses the identity of the main table as key
                        ForeignKey fk = new ForeignKey {
                            Name = "FK_" + tableName + SQLBase.SubTableKeyColumn + "_" + ForeignKeyTable + "_" + identityName
                        };
                        fk.ForeignKeyColumns.Add(new ForeignKeyColumn { Column = SQLBase.SubTableKeyColumn, ReferencedColumn = identityName });
                        fk.ReferencedTable = ForeignKeyTable;
                        newTable.ForeignKeys.Add(fk);
                    } else {
                        if (key2Name != null) throw new InternalError("Only a single key can be used with foreign keys");

                        ForeignKey fk = new ForeignKey {
                            Name = "FK_" + tableName + "_" + key1Name + "_" + ForeignKeyTable + "_" + key1Name
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

        private bool AddTableColumns(string dbName, string dbOwner, TableInfo tableInfo,
                string key1Name, string key2Name, string identityName,
                List<PropertyData> propData, Type tpContainer, string prefix, bool topMost, List<string> columns, List<string> errorList,
                bool SubTable = false) {

            Table newTable = tableInfo.NewTable;
            Table currentTable = tableInfo.CurrentTable;
            bool hasSubTable = false;

            foreach (PropertyData prop in propData) {

                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {

                    string colName = prefix + prop.ColumnName;
                    if (colName == key1Name || colName == key2Name || !columns.Contains(colName)) {

                        columns.Add(colName);

                        if (prop.Name == identityName) {
                            if (SubTable)
                                throw new InternalError("Subtables can't have an explicit identity");
                            if (pi.PropertyType != typeof(int))
                                throw new InternalError("Identity columns must be of type int");
                            // done by caller
                        } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                            if (topMost && (prop.Name == key1Name || prop.Name == key2Name))
                                throw new InternalError("Binary data can't be a primary key - table {0}", newTable.Name);
                            Column newColumn = new Column {
                                Name = colName,
                                DataType = SqlDbType.VarBinary,
                                Nullable = true,
                            };
                            newTable.Columns.Add(newColumn);
                        } else if (pi.PropertyType == typeof(MultiString)) {
                            if (Languages.Count == 0)
                                throw new InternalError("We need Languages for MultiString support");
                            foreach (var lang in Languages) {
                                colName = prefix + SQLBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name);
                                Column newColumn = new Column {
                                    Name = colName,
                                    DataType = SqlDbType.NVarChar,
                                };
                                StringLengthAttribute attr = (StringLengthAttribute)pi.GetCustomAttribute(typeof(StringLengthAttribute));
                                if (attr == null)
                                    throw new InternalError("StringLength attribute missing for property {0}", prefix + prop.Name);
                                if (attr.MaximumLength >= 4000)
                                    newColumn.Length = 0;
                                else
                                    newColumn.Length = attr.MaximumLength;
                                if (colName != key1Name && colName != key2Name)
                                    newColumn.Nullable = true;
                                newTable.Columns.Add(newColumn);
                            }
                        } else if (pi.PropertyType == typeof(Image)) {
                            if (topMost && (prop.Name == key1Name || prop.Name == key2Name))
                                throw new InternalError("Image can't be a primary key - table {0}", newTable.Name);
                            Column newColumn = new Column {
                                Name = colName,
                                DataType = SqlDbType.VarBinary,
                                Nullable = true,
                            };
                            newTable.Columns.Add(newColumn);
                        } else if (TryGetDataType(pi.PropertyType)) {
                            Column newColumn = new Column {
                                Name = colName,
                                Nullable = true,
                            };
                            newColumn.DataType = GetDataType(pi);
                            if (pi.PropertyType == typeof(string)) {
                                StringLengthAttribute attr = (StringLengthAttribute)pi.GetCustomAttribute(typeof(StringLengthAttribute));
                                if (attr == null)
                                    throw new InternalError("StringLength attribute missing for property {0}", pi.Name);
                                int len = attr.MaximumLength;
                                if (len == 0 || len >= 4000)
                                    newColumn.Length = 0;
                                else
                                    newColumn.Length = len;
                            }
                            bool nullable = false;
                            if (colName != key1Name && colName != key2Name && (pi.PropertyType == typeof(string) || Nullable.GetUnderlyingType(pi.PropertyType) != null))
                                nullable = true;
                            newColumn.Nullable = nullable;
                            Data_NewValue newValAttr = (Data_NewValue)pi.GetCustomAttribute(typeof(Data_NewValue));
                            if (currentTable != null && !HasColumn(currentTable, colName)) {
                                if (newValAttr == null)
                                    throw new InternalError("Property {0} in table {1} doesn't have a Data_NewValue attribute, which is required when updating tables", prop.Name, newTable.Name);
                            }
                            newTable.Columns.Add(newColumn);
                        } else if (pi.PropertyType.IsClass && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                            // This is a enumerated type, so we have to create a separate table using this table's identity column as a link
                            if (SubTable) throw new InternalError("Nested subtables not supported");
                            // determine the enumerated type
                            Type subType = pi.PropertyType.GetInterfaces().Where(t => t.IsGenericType == true && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                                    .Select(t => t.GetGenericArguments()[0]).FirstOrDefault();
                            // create a table that links the main table and this enumerated type using the key of the table
                            string subTableName = newTable.Name + "_" + pi.Name;
                            List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subType);

                            TableInfo subTableInfo = CreateSimpleTableFromModel(dbName, dbOwner, subTableName, SQLBase.SubTableKeyColumn, null,
                                HasIdentity(identityName) ? identityName : SQLBase.IdentityColumn, subPropData, subType, errorList, columns,
                                TopMost: false,
                                ForeignKeyTable: newTable.Name,
                                SubTable: true,
                                SiteSpecific: false);
                            if (subTableInfo == null)
                                throw new InternalError($"Creation of subtable {subTableName} failed");
                            tableInfo.SubTables.Add(subTableInfo);

                            hasSubTable = true;
                        } else if (pi.PropertyType.IsClass) {
                            List<PropertyData> subPropData = ObjectSupport.GetPropertyData(pi.PropertyType);
                            AddTableColumns(dbName, dbOwner, tableInfo, null, null, identityName, subPropData, pi.PropertyType, prefix + prop.Name + "_", SubTable, columns, errorList);
                        } else
                            throw new InternalError("Unknown property type {2} used in class {0}, property {1}", tpContainer.FullName, prop.Name, pi.PropertyType.FullName);
                    }
                }
            }
            return hasSubTable;
        }

        private void MakeTables(string dbName, string dbOwner, TableInfo tableInfo) {
            if (tableInfo.CurrentTable != null) {
                RemoveIndexesAndForeignKeys(dbName, dbOwner, tableInfo);
            }
            MakeTable(dbName, dbOwner, tableInfo);
            MakeForeignKeys(dbName, dbOwner, tableInfo);// we can't make foreign keys until all tables have been created/updated
        }

        private void MakeTable(string dbName, string dbOwner, TableInfo tableInfo) {
            if (tableInfo.CurrentTable != null)
                UpdateTable(dbName, dbOwner, tableInfo.CurrentTable, tableInfo.NewTable);
            else
                CreateTable(dbName, dbOwner, tableInfo.NewTable);
            foreach (TableInfo subtableInfo in tableInfo.SubTables)
                MakeTable(dbName, dbOwner, subtableInfo);
        }

        private void MakeForeignKeys(string dbName, string dbOwner, TableInfo tableInfo) {
            if (tableInfo.CurrentTable != null)
                UpdateForeignKeys(dbName, dbOwner, tableInfo.CurrentTable, tableInfo.NewTable);
            else
                CreateForeignKey(dbName, dbOwner, tableInfo.NewTable);
            foreach (TableInfo subtableInfo in tableInfo.SubTables)
                MakeForeignKeys(dbName, dbOwner, subtableInfo);
        }

        private void CreateTable(string dbName, string dbOwner, Table newTable) {

            StringBuilder sb = new StringBuilder();
            StringBuilder sbIx = new StringBuilder();
            sb.Append("SET ANSI_NULLS ON;\r\n");
            sb.Append("SET ANSI_PADDING ON;\r\n");
            sb.Append("SET QUOTED_IDENTIFIER ON;\r\n\r\n");

            sb.Append($"CREATE TABLE[{dbOwner}].[{newTable.Name}](\r\n");
            // Columns
            foreach (Column col in newTable.Columns) {
                sb.Append($"    [{col.Name}] {GetDataTypeString(col)}{GetIdentity(col)}{GetNullable(col)},\r\n");
            }

            // Indexes

            foreach (Index index in newTable.Indexes) {
                switch (index.IndexType) {
                    case IndexType.PrimaryKey:
                    case IndexType.UniqueKey:
                        sb.Append(GetAddIndex(index, dbName, dbOwner, newTable));
                        break;
                    case IndexType.Indexed:
                        // created separately, after table
                        sbIx.Append(GetAddIndex(index, dbName, dbOwner, newTable));
                        break;
                }
            }
            sb.RemoveLastComma();

            sb.Append("\r\n) ON[PRIMARY];\r\n\r\n");

            sb.Append(sbIx.ToString());

            sb.Append("SET ANSI_PADDING OFF;\r\n");

            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = Conn;
                cmd.CommandText = sb.ToString();
                cmd.ExecuteNonQuery();
            }
        }

        private void CreateForeignKey(string dbName, string dbOwner, Table newTable) {

            StringBuilder sb = new StringBuilder();
            sb.Append("SET QUOTED_IDENTIFIER ON;\r\n\r\n");

            foreach (ForeignKey fk in newTable.ForeignKeys) {
                sb.Append(GetAddForeignKey(fk, dbName, dbOwner, newTable));
            }

            using (SqlCommand cmd = new SqlCommand()) {
                cmd.Connection = Conn;
                cmd.CommandText = sb.ToString();
                cmd.ExecuteNonQuery();
            }
        }

        private void RemoveIndexesAndForeignKeys(string dbName, string dbOwner, TableInfo tableInfo) {

            foreach (TableInfo t in tableInfo.SubTables) {
                RemoveIndexesAndForeignKeys(dbName, dbOwner, t);
            }

            Table currentTable = tableInfo.CurrentTable;
            Table newTable = tableInfo.NewTable;

            List<Index> removedIndexes;
            List<ForeignKey> removedForeignKeys;

            if (!MajorDataChange) {
                removedIndexes = currentTable.Indexes.Except(newTable.Indexes, new IndexComparer()).ToList();
                removedForeignKeys = currentTable.ForeignKeys.Except(newTable.ForeignKeys, new ForeignKeyComparer()).ToList();
            } else {
                removedIndexes = currentTable.Indexes;
                removedForeignKeys = currentTable.ForeignKeys;
            }

            StringBuilder sb = new StringBuilder();

            // Remove index
            foreach (Index index in removedIndexes) {
                switch (index.IndexType) {
                    case IndexType.Indexed:
                        sb.Append($"DROP INDEX [{index.Name}] ON [{dbOwner}].[{newTable.Name}];\r\n");
                        break;
                    case IndexType.UniqueKey:
                        sb.Append($"ALTER TABLE [{dbOwner}].[{newTable.Name}] DROP CONSTRAINT [{index.Name}];\r\n");
                        break;
                    case IndexType.PrimaryKey:
                        sb.Append($"ALTER TABLE [{dbOwner}].[{newTable.Name}] DROP CONSTRAINT [{index.Name}];\r\n");
                        break;
                }
            }

            // Remove foreign key
            foreach (ForeignKey fk in removedForeignKeys) {
                sb.Append($"ALTER TABLE [{dbOwner}].[{newTable.Name}] DROP CONSTRAINT [{fk.Name}];\r\n");
            }

            if (sb.Length != 0) {
                using (SqlCommand cmd = new SqlCommand()) {
                    cmd.Connection = Conn;
                    cmd.CommandText = sb.ToString();
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void UpdateTable(string dbName, string dbOwner, Table currentTable, Table newTable) {

            StringBuilder sb = new StringBuilder();

            List<Column> removedColumns = currentTable.Columns.Except(newTable.Columns, new ColumnComparer()).ToList();
            List<Column> addedColumns = newTable.Columns.Except(currentTable.Columns, new ColumnComparer()).ToList();
            List<Column> alteredColumns = addedColumns.Intersect(removedColumns, new ColumnNameComparer()).ToList();
            removedColumns = removedColumns.Except(alteredColumns, new ColumnNameComparer()).ToList();
            addedColumns = addedColumns.Except(alteredColumns, new ColumnNameComparer()).ToList();

            List<Index> addedIndexes;
            if (!MajorDataChange) {
                addedIndexes = newTable.Indexes.Except(currentTable.Indexes, new IndexComparer()).ToList();
            } else {
                addedIndexes = newTable.Indexes;
            }

            // Remove columns
            foreach (Column col in removedColumns) {
                sb.Append($@"
IF EXISTS (SELECT * FROM  [{dbOwner}].[sysobjects] WHERE id = OBJECT_ID(N'DF_{currentTable.Name}_{col.Name}') AND type = 'D')
    BEGIN
       ALTER TABLE  [{dbOwner}].[{newTable.Name}] DROP CONSTRAINT [DF_{currentTable.Name}_{col.Name}], COLUMN [{col.Name}];
    END
ELSE
    BEGIN
       ALTER TABLE  [{dbOwner}].[{newTable.Name}] DROP COLUMN [{col.Name}];
    END
");
            }

            // Add columns
            foreach (Column col in addedColumns) {
                sb.Append($"ALTER TABLE [{dbOwner}].[{newTable.Name}] ADD [{col.Name}] {GetDataTypeString(col)}{GetDataTypeDefault(newTable.Name, col)}{GetIdentity(col)}{GetNullable(col)};\r\n");
            }
            // Altered columns
            foreach (Column col in alteredColumns) {
                sb.Append($"ALTER TABLE [{dbOwner}].[{newTable.Name}] ALTER [{col.Name}] {GetDataTypeString(col)}{GetDataTypeDefault(newTable.Name, col)}{GetIdentity(col)}{GetNullable(col)};\r\n");
            }

            // Add index
            foreach (Index index in addedIndexes) {
                switch (index.IndexType) {
                    case IndexType.PrimaryKey:
                    case IndexType.UniqueKey:
                        sb.Append($"ALTER TABLE [{dbOwner}].[{newTable.Name}]\r\n");
                        sb.Append($"  ADD");
                        sb.Append(GetAddIndex(index, dbName, dbOwner, newTable));
                        sb.RemoveLastComma();
                        sb.Append($";\r\n");
                        break;
                    case IndexType.Indexed:
                        sb.Append(GetAddIndex(index, dbName, dbOwner, newTable));
                        break;
                }
            }

            if (sb.Length != 0) {
                using (SqlCommand cmd = new SqlCommand()) {
                    cmd.Connection = Conn;
                    cmd.CommandText = sb.ToString();
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void UpdateForeignKeys(string dbName, string dbOwner, Table currentTable, Table newTable) {

            StringBuilder sb = new StringBuilder();

            List<ForeignKey> addedForeignKeys;
            if (!MajorDataChange)
                addedForeignKeys = newTable.ForeignKeys.Except(currentTable.ForeignKeys, new ForeignKeyComparer()).ToList();
            else
                addedForeignKeys = newTable.ForeignKeys;

            foreach (ForeignKey fk in addedForeignKeys) {
                sb.Append(GetAddForeignKey(fk, dbName, dbOwner, newTable));
            }

            if (sb.Length != 0) {
                using (SqlCommand cmd = new SqlCommand()) {
                    cmd.Connection = Conn;
                    cmd.CommandText = sb.ToString();
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private bool HasColumn(Table table, string name) {
            return (from c in table.Columns where c.Name == name select c).FirstOrDefault() != null;
        }

        protected bool TryGetDataType(Type tp) {
            if (tp == typeof(DateTime) || tp == typeof(DateTime?))
                return true;
            else if (tp == typeof(TimeSpan) || tp == typeof(TimeSpan?))
                return true;
            else if (tp == typeof(decimal) || tp == typeof(decimal?))
                return true;
            else if (tp == typeof(bool) || tp == typeof(bool?))
                return true;
            else if (tp == typeof(System.Guid) || tp == typeof(System.Guid?))
                return true;
            else if (tp == typeof(Image))
                return true;
            else if (tp == typeof(int) || tp == typeof(int?))
                return true;
            else if (tp == typeof(long) || tp == typeof(long?))
                return true;
            else if (tp == typeof(Single) || tp == typeof(Single?))
                return true;
            else if (tp == typeof(string))
                return true;
            else if (tp.IsEnum)
                return true;
            return false;
        }
        private SqlDbType GetDataType(PropertyInfo pi) {
            Type tp = pi.PropertyType;
            if (tp == typeof(DateTime) || tp == typeof(DateTime?))
                return SqlDbType.DateTime2;
            else if (tp == typeof(TimeSpan) || tp == typeof(TimeSpan?))
                return SqlDbType.BigInt;
            else if (tp == typeof(decimal) || tp == typeof(decimal?))
                return SqlDbType.Money;
            else if (tp == typeof(bool) || tp == typeof(bool?))
                return SqlDbType.Bit;
            else if (tp == typeof(System.Guid) || tp == typeof(System.Guid?))
                return SqlDbType.UniqueIdentifier;
            else if (tp == typeof(Image))
                return SqlDbType.VarBinary;
            else if (tp == typeof(int) || tp == typeof(int?))
                return SqlDbType.Int;
            else if (tp == typeof(long) || tp == typeof(long?))
                return SqlDbType.BigInt;
            else if (tp == typeof(Single) || tp == typeof(Single?))
                return SqlDbType.Float;
            else if (tp.IsEnum)
                return SqlDbType.Int;
            else if (tp == typeof(string))
                return SqlDbType.NVarChar;
            throw new InternalError("Unsupported property type {0} for property {1}", tp.FullName, pi.Name);
        }
        private string GetDataTypeString(Column col) {
            switch (col.DataType) {
                case SqlDbType.BigInt:
                    return "[bigint]";
                case SqlDbType.Bit:
                    return "[bit]";
                case SqlDbType.DateTime2:
                    return "[datetime2](7)";
                case SqlDbType.Money:
                    return "[money]";
                case SqlDbType.UniqueIdentifier:
                    return "[uniqueidentifier]";
                case SqlDbType.VarBinary:
                    return "[varbinary](max)";
                case SqlDbType.Int:
                    return "int";
                case SqlDbType.Float:
                    return "float";
                case SqlDbType.NVarChar:
                    if (col.Length == 0)
                        return "[nvarchar](max)";
                    else
                        return $"[nvarchar]({col.Length})";
                default:
                    throw new InternalError($"Column {col.Name} has unsupported type name {col.DataType.ToString()}");
            }
        }
        private string GetDataTypeDefault(string tableName, Column col) {
            if (col.Nullable)
                return "";

            switch (col.DataType) {
                case SqlDbType.BigInt:
                    return $" CONSTRAINT [DF_{tableName}_{col.Name}] DEFAULT 0";
                case SqlDbType.Bit:
                    return $" CONSTRAINT [DF_{tableName}_{col.Name}] DEFAULT 0";
                case SqlDbType.DateTime2:
                    return "";
                case SqlDbType.Money:
                    return $" CONSTRAINT [DF_{tableName}_{col.Name}] DEFAULT 0";
                case SqlDbType.UniqueIdentifier:
                    return "";
                case SqlDbType.VarBinary:
                    return "";
                case SqlDbType.Int:
                    return $" CONSTRAINT [DF_{tableName}_{col.Name}] DEFAULT 0";
                case SqlDbType.Float:
                    return $" CONSTRAINT [DF_{tableName}_{col.Name}] DEFAULT 0";
                case SqlDbType.NVarChar:
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
                return $" IDENTITY({col.IdentitySeed}, {col.IdentityIncrement}) NOT FOR REPLICATION";
            }
            return "";
        }
        private string GetAddIndex(Index index, string dbName, string dbOwner, Table newTable) {
            StringBuilder sb = new StringBuilder();
            sb.Append($"");
            switch (index.IndexType) {
                case IndexType.PrimaryKey:
                    sb.Append($"  CONSTRAINT [{index.Name}] PRIMARY KEY CLUSTERED (\r\n");
                    foreach (string col in index.IndexedColumns) {
                        sb.Append($"    [{col}] ASC,\r\n");
                    }
                    sb.RemoveLastComma();
                    sb.Append($"\r\n  ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],\r\n");
                    break;
                case IndexType.UniqueKey:
                    sb.Append($"  CONSTRAINT [{index.Name}] UNIQUE NONCLUSTERED (\r\n");
                    foreach (string col in index.IndexedColumns) {
                        sb.Append($"    [{col}] ASC,\r\n");
                    }
                    sb.RemoveLastComma();
                    sb.Append($"\r\n  ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],\r\n");
                    break;
                case IndexType.Indexed:
                    // created separately, after table
                    sb.Append($"CREATE NONCLUSTERED INDEX [{index.Name}] ON [{dbOwner}].[{newTable.Name}] (\r\n");
                    foreach (string col in index.IndexedColumns) {
                        sb.Append($"    [{col}] ASC,\r\n");
                    }
                    sb.RemoveLastComma();
                    sb.Append($"\r\n  ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];\r\n\r\n");
                    break;
            }
            return sb.ToString();
        }
        private string GetAddForeignKey(ForeignKey fk, string dbName, string dbOwner, Table newTable) {
            StringBuilder sb = new StringBuilder();
            sb.Append($"ALTER TABLE [{dbOwner}].[{newTable.Name}]  WITH CHECK ADD  CONSTRAINT [{fk.Name}]\r\n");
            sb.Append($"  FOREIGN KEY(\r\n");
            foreach (ForeignKeyColumn fkCol in fk.ForeignKeyColumns) {
                sb.Append($"    [{fkCol.Column}],\r\n");
            }
            sb.RemoveLastComma();
            sb.Append($"\r\n  )\r\n");

            sb.Append($"  REFERENCES[{dbOwner}].[{fk.ReferencedTable}] (\r\n");
            foreach (ForeignKeyColumn fkCol in fk.ForeignKeyColumns) {
                sb.Append($"    [{fkCol.ReferencedColumn}],\r\n");
            }
            sb.RemoveLastComma();
            sb.Append($"\r\n  )\r\n");
            sb.Append($"  ON DELETE CASCADE;\r\n\r\n");

            sb.Append($"ALTER TABLE [{dbOwner}].[{newTable.Name}] CHECK CONSTRAINT [{fk.Name}];\r\n\r\n");
            return sb.ToString();
        }
    }
}