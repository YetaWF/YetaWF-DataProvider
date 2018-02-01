/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Reflection;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Language;
using YetaWF.Core.Models;
using YetaWF.Core.Models.Attributes;
using YetaWF.Core.Packages;
using YetaWF.Core.Support;
using YetaWF.DataProvider.SQL2;

namespace YetaWF.DataProvider {

    public class SQLCreate {

        public int IdentitySeed { get; private set; }
        public bool Logging { get; private set; }
        public List<LanguageData> Languages { get; private set; }

        public SQLCreate(List<LanguageData> languages, int identitySeed, bool logging) {
            Languages = languages;
            IdentitySeed = identitySeed;
            Logging = logging;
        }

        List<ForeignKey> SavedNewKeys = null;
        private static List<string> DBsCompleted;

        protected bool HasIdentity(string identityName) {
            return !string.IsNullOrWhiteSpace(identityName);
        }

        public bool CreateTable(Database db, string dbOwner, string tableName, string key1Name, string key2Name, string identityName, List<PropertyData> propData, Type tpProps,
                List<string> errorList, List<string> columns,
                bool TopMost = false,
                bool SiteSpecific = false,
                string ForeignKeyTable = null,
                string DerivedDataTableName = null, string DerivedDataTypeName = null, string DerivedAssemblyName = null,
                bool SubTable = false) {
            try {
                RemoveIndexesIfNeeded(db);
                Table newTab = null;
                List<string> origColumns, origIndexes;
                bool updatingTable;
                if (db.Tables.Contains(tableName)) {
                    updatingTable = true;
                    newTab = db.Tables[tableName];
                    if (newTab.Schema != dbOwner)
                        throw new InternalError("Can't change table {0} schema {1} to {2}", tableName, newTab.Schema, dbOwner);
                    origIndexes = (from Index i in newTab.Indexes select i.Name).ToList();
                    origColumns = (from Column c in newTab.Columns select c.Name).ToList();
                } else {
                    updatingTable = false;
                    newTab = new Table(db, tableName);
                    newTab.Schema = dbOwner;
                    origIndexes = new List<string>();
                    origColumns = new List<string>();
                }
                List<Column> savedColumnsWithConstraints = new List<Column>();
                if (TopMost)
                    SavedNewKeys = new List<ForeignKey>();

                bool hasSubTable = AddTableColumns(db, dbOwner, updatingTable, origColumns, origIndexes, newTab, tableName, key1Name, key2Name, identityName, propData, tpProps, "", true, columns, errorList, savedColumnsWithConstraints, SubTable: SubTable);

                // if this table (base class) has a derived type, add its table name and its derived type as a column
                if (DerivedDataTableName != null) {
                    Column newColumn = MakeColumn(origColumns, newTab, DerivedDataTableName);
                    newColumn.DataType = Microsoft.SqlServer.Management.Smo.DataType.NVarChar(80);
                    newColumn.Nullable = false;

                    newColumn = MakeColumn(origColumns, newTab, DerivedDataTypeName);
                    newColumn.DataType = Microsoft.SqlServer.Management.Smo.DataType.NVarChar(200);
                    newColumn.Nullable = false;

                    newColumn = MakeColumn(origColumns, newTab, DerivedAssemblyName);
                    newColumn.DataType = Microsoft.SqlServer.Management.Smo.DataType.NVarChar(200);
                    newColumn.Nullable = false;
                }
                if (SiteSpecific) {
                    Column newColumn = MakeColumn(origColumns, newTab, SQL2Base.SiteColumn);
                    newColumn.DataType = Microsoft.SqlServer.Management.Smo.DataType.Int;
                    newColumn.Nullable = false;
                }
                // PK Index
                if (!SubTable) //$$ was ForeignKeyTable == null) 
                {
                    Index index = MakeIndex(origIndexes, newTab, "PK_" + tableName, key1Name, ColumnName2: key2Name, AddSiteKey: SiteSpecific);
                    index.IndexKeyType = Microsoft.SqlServer.Management.Smo.IndexKeyType.DriPrimaryKey;
                }
                // Other indexes
                {
                    List<PropertyData> propIndexes = (
                        from p in propData
                        where p.HasAttribute(Data_IndexAttribute.AttributeName)
                        select p).ToList();
                    if (propIndexes != null) {
                        foreach (var propIndex in propIndexes) {
                            if (propIndex.PropInfo.PropertyType == typeof(MultiString)) {
                                if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                                MultiString ms = new MultiString();
                                foreach (var lang in Languages) {
                                    string col = SQL2Base.ColumnFromPropertyWithLanguage(lang.Id, propIndex.Name);
                                    Index index = MakeIndex(origIndexes, newTab, "K_" + tableName + "_" + col, col);
                                    index.IndexKeyType = Microsoft.SqlServer.Management.Smo.IndexKeyType.None;
                                }
                            } else {
                                Index index = MakeIndex(origIndexes, newTab, "K_" + tableName + "_" + propIndex.Name, propIndex.Name);
                                index.IndexKeyType = Microsoft.SqlServer.Management.Smo.IndexKeyType.None;
                            }
                        }
                    }
                }
                if (SubTable) { // for replication
                    Column newColumn = MakeColumn(origColumns, newTab, SQL2Base.IdentityColumn);
                    newColumn.DataType = Microsoft.SqlServer.Management.Smo.DataType.Int;
                    newColumn.Nullable = false;
                    newColumn.Identity = true;
                    newColumn.IdentityIncrement = 1;
                    newColumn.IdentitySeed = 0;

                    Index index = MakeIndex(origIndexes, newTab, "K_" + tableName + "_" + SQL2Base.IdentityColumn, SQL2Base.IdentityColumn);
                    index.IndexKeyType = Microsoft.SqlServer.Management.Smo.IndexKeyType.DriUniqueKey;
                } else {
                    if (HasIdentity(identityName)) {
                        Column newColumn = MakeColumn(origColumns, newTab, identityName);
                        newColumn.DataType = Microsoft.SqlServer.Management.Smo.DataType.Int;
                        newColumn.Nullable = false;
                        newColumn.Identity = true;
                        newColumn.IdentityIncrement = 1;
                        newColumn.IdentitySeed = IdentitySeed;

                        Index index = MakeIndex(origIndexes, newTab, "K_" + tableName + "_" + identityName, identityName);
                        index.IndexKeyType = Microsoft.SqlServer.Management.Smo.IndexKeyType.DriUniqueKey;
                    } else {
                        if (hasSubTable) {
                            Column newColumn = MakeColumn(origColumns, newTab, SQL2Base.IdentityColumn);
                            newColumn.DataType = Microsoft.SqlServer.Management.Smo.DataType.Int;
                            newColumn.Nullable = false;
                            newColumn.Identity = true;
                            newColumn.IdentityIncrement = 1;
                            newColumn.IdentitySeed = 0;

                            Index index = MakeIndex(origIndexes, newTab, "K_" + tableName + "_" + SQL2Base.IdentityColumn, SQL2Base.IdentityColumn);
                            index.IndexKeyType = Microsoft.SqlServer.Management.Smo.IndexKeyType.DriUniqueKey;
                        }
                    }
                }
                if (ForeignKeyTable != null) {
                    if (SubTable) {
                        // a subtable uses the identity of the main table as key so we have to create the column as it's not part of the data
                        Column newColumn = MakeColumn(origColumns, newTab, SQL2Base.SubTableKeyColumn);
                        newColumn.DataType = Microsoft.SqlServer.Management.Smo.DataType.Int;
                        newColumn.Nullable = false;

                        Index index = MakeIndex(origIndexes, newTab, "K_" + tableName + "_" + SQL2Base.SubTableKeyColumn, SQL2Base.SubTableKeyColumn);
                        index.IndexKeyType = Microsoft.SqlServer.Management.Smo.IndexKeyType.None;

                        if (TopMost) throw new InternalError("Topmost CreateTable call can't define a SubTable");
                        if (!HasIdentity(identityName)) throw new InternalError("Identity required");
                        // a subtable uses the identity of the main table as key
                        MakeForeignKey(newTab, "FK_" + tableName + SQL2Base.SubTableKeyColumn + "_" + ForeignKeyTable + "_" + identityName, SQL2Base.SubTableKeyColumn, ForeignKeyTable, identityName);
                    } else {
                        if (key2Name != null) throw new InternalError("Only a single key can be used with foreign keys");
                        MakeForeignKey(newTab, "FK_" + tableName + "_" + key1Name + "_" + ForeignKeyTable + "_" + key1Name, key1Name, ForeignKeyTable, key1Name, AddSiteKey: true);
                    }
                }

                if (updatingTable) {
                    // remove indexes that no longer exist
                    foreach (string origIndex in origIndexes)
                        newTab.Indexes[origIndex].Drop();
                    // remove columns that no longer exist
                    foreach (string origCol in origColumns)
                        newTab.Columns[origCol].Drop();
                    newTab.Alter();
                    // remove default values we added for existing records
                    if (savedColumnsWithConstraints.Count > 0) {
                        foreach (Column column in savedColumnsWithConstraints)
                            column.DefaultConstraint.Drop();
                        newTab.Alter();
                    }
                } else {
                    newTab.Create();
                }

                // create all the foreign keys (this will also create the saved keys for subtables
                if (TopMost) {
                    foreach (var fk in SavedNewKeys) {
                        fk.Create();
                    }
                    SavedNewKeys = null;
                }
                return true;

            } catch (Exception exc) {
                if (Logging) YetaWF.Core.Log.Logging.AddErrorLog("Couldn't create table {0}", tableName, exc);
                errorList.Add(string.Format("Couldn't create table {0}", tableName));
                while (exc != null && exc.Message != null) {
                    errorList.Add(exc.Message);
                    exc = exc.InnerException;
                }
                return false;
            }
        }

        private Column MakeColumn(List<string> origColumns, Table table, string name) {
            Column newColumn;
            if (table.Columns.Contains(name)) {
                newColumn = table.Columns[name];
            } else {
                newColumn = new Column(table, name);
                table.Columns.Add(newColumn);
            }
            if (origColumns.Contains(name))
                origColumns.Remove(name);
            return newColumn;
        }
        private bool IsNewColumn(Table table, string name) {
            return !table.Columns.Contains(name);
        }
        private Index MakeIndex(List<string> origIndexes, Table table, string indexName, string columnName, string ColumnName2 = null, bool AddSiteKey = false) {
            Index newIndex;
            if (table.Indexes.Contains(indexName)) {
                newIndex = table.Indexes[indexName];
                if (!newIndex.IndexedColumns.Contains(columnName))
                    throw new InternalError("Changing index/column name is not supported");
                if (!string.IsNullOrWhiteSpace(ColumnName2)) {
                    if (!newIndex.IndexedColumns.Contains(ColumnName2))
                        throw new InternalError("Changing index/column name is not supported");
                }
                if (AddSiteKey && !newIndex.IndexedColumns.Contains(SQL2Base.SiteColumn))
                    throw new InternalError("Changing site dependency is not supported");
            } else {
                newIndex = new Index(table, indexName);
                table.Indexes.Add(newIndex);
                newIndex.IndexedColumns.Add(new IndexedColumn(newIndex, columnName));
                if (!string.IsNullOrWhiteSpace(ColumnName2))
                    newIndex.IndexedColumns.Add(new IndexedColumn(newIndex, ColumnName2));
                if (AddSiteKey)
                    newIndex.IndexedColumns.Add(new IndexedColumn(newIndex, SQL2Base.SiteColumn));
            }
            if (origIndexes.Contains(indexName))
                origIndexes.Remove(indexName);
            return newIndex;
        }
        private ForeignKey MakeForeignKey(Table table, string fkName, string column, string referencedTable, string referencedColumn, bool AddSiteKey = false) {
            ForeignKey fk;
            if (table.ForeignKeys.Contains(fkName)) {
                fk = table.ForeignKeys[fkName];
                if (!fk.Columns.Contains(column))
                    throw new InternalError("Changing existing foreign key columns is not supported");
                if (AddSiteKey && !fk.Columns.Contains(SQL2Base.SiteColumn))
                    throw new InternalError("Changing existing foreign key site dependency is not supported");
            } else {
                fk = new ForeignKey(table, fkName);
                ForeignKeyColumn fkc = new ForeignKeyColumn(fk, column, referencedColumn);
                fk.Columns.Add(fkc);
                fk.ReferencedTable = referencedTable;
                fk.DeleteAction = ForeignKeyAction.Cascade;
                if (AddSiteKey) {
                    ForeignKeyColumn fkcSite = new ForeignKeyColumn(fk, SQL2Base.SiteColumn, SQL2Base.SiteColumn);
                    fk.Columns.Add(fkcSite);
                }
                SavedNewKeys.Add(fk);
            }
            return fk;
        }

        private bool AddTableColumns(Database db, string dbOwner, bool updatingTable, List<string> origColumns, List<string> origIndexes, Table newTab,
                string tableName, string key1Name, string key2Name, string identityName,
                List<PropertyData> propData, Type tpContainer, string prefix, bool topMost, List<string> columns, List<string> errorList,
                List<Column> savedColumnsWithConstraints,
                bool SubTable = false) {

            bool hasSubTable = false;
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    string colName = prefix + prop.Name;
                    if (colName == key1Name || colName == key2Name || !columns.Contains(colName)) {
                        columns.Add(colName);
                        bool isNew = IsNewColumn(newTab, colName);
                        if (prop.Name == identityName) {
                            if (SubTable) throw new InternalError("Subtables can't have an explicit identity");
                            if (pi.PropertyType != typeof(int)) throw new InternalError("Identity columns must be of type int");
                            // done by caller
                        } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                            if (topMost && (prop.Name == key1Name || prop.Name == key2Name))
                                throw new InternalError("Binary data can't be a primary key - table {0}", tableName);
                            Column newColumn = MakeColumn(origColumns, newTab, colName);
                            newColumn.DataType = Microsoft.SqlServer.Management.Smo.DataType.VarBinaryMax;
                            newColumn.Nullable = true;
                        } else if (pi.PropertyType == typeof(MultiString)) {
                            if (Languages.Count == 0)
                                throw new InternalError("We need Languages for MultiString support");
                            foreach (var lang in Languages) {
                                colName = prefix + SQL2Base.ColumnFromPropertyWithLanguage(lang.Id, prop.Name);
                                Column newColumn = MakeColumn(origColumns, newTab, colName);
                                StringLengthAttribute attr = (StringLengthAttribute) pi.GetCustomAttribute(typeof(StringLengthAttribute));
                                if (attr == null)
                                    throw new InternalError("StringLength attribute missing for property {0}", prefix + prop.Name);
                                if (attr.MaximumLength >= 4000)
                                    newColumn.DataType = Microsoft.SqlServer.Management.Smo.DataType.NVarCharMax;
                                else
                                    newColumn.DataType = Microsoft.SqlServer.Management.Smo.DataType.NVarChar(attr.MaximumLength);
                                if (colName != key1Name && colName != key2Name)
                                    newColumn.Nullable = true;
                            }
                        } else if (pi.PropertyType == typeof(Image)) {
                            if (topMost && (prop.Name == key1Name || prop.Name == key2Name))
                                throw new InternalError("Image can't be a primary key - table {0}", tableName);
                            Column newColumn = MakeColumn(origColumns, newTab, colName);
                            newColumn.DataType = Microsoft.SqlServer.Management.Smo.DataType.VarBinaryMax;
                            newColumn.Nullable = true;
                        } else if (TryGetDataType(pi.PropertyType)) {
                            Column newColumn = MakeColumn(origColumns, newTab, colName);
                            newColumn.DataType = GetDataType(pi);
                            bool nullable = false;
                            if (colName != key1Name && colName != key2Name && (pi.PropertyType == typeof(string) || Nullable.GetUnderlyingType(pi.PropertyType) != null))
                                nullable = true;
                            newColumn.Nullable = nullable;
                            Data_NewValue newValAttr = (Data_NewValue)pi.GetCustomAttribute(typeof(Data_NewValue));
                            if (updatingTable && isNew) {
                                if (newValAttr != null) {
                                    savedColumnsWithConstraints.Add(newColumn);
                                    newColumn.AddDefaultConstraint().Text = newValAttr.Value;
                                } else if (!nullable) {
                                    throw new InternalError("Non-nullable property {0} in table {1} doesn't have a Data_NewValue attribute, which is required when updating tables", prop.Name, tableName);
                                }
                            }
                        } else if (pi.PropertyType.IsClass && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                            // This is a enumerated type, so we have to create a separate table using this table's identity column as a link
                            if (SubTable) throw new InternalError("Nested subtables not supported");
                            // determine the enumerated type
                            Type subType = pi.PropertyType.GetInterfaces().Where(t => t.IsGenericType == true && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                                    .Select(t => t.GetGenericArguments()[0]).FirstOrDefault();
                            // create a table that links the main table and this enumerated type using the key of the table
                            string subTableName = newTab.Name + "_" + pi.Name;
                            List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subType);
                            bool success = CreateTable(db, dbOwner, subTableName, SQL2Base.SubTableKeyColumn, null,
                                HasIdentity(identityName) ? identityName : SQL2Base.IdentityColumn, subPropData, subType, errorList, columns,
                                TopMost: false,
                                ForeignKeyTable: tableName, 
                                SubTable: true, 
                                SiteSpecific: false);
                            if (!success)
                                throw new InternalError("Creation of subtable failed");
                            hasSubTable = true;
                        } else if (pi.PropertyType.IsClass) {
                            List<PropertyData> subPropData = ObjectSupport.GetPropertyData(pi.PropertyType);
                            AddTableColumns(db, dbOwner, updatingTable, origColumns, origIndexes, newTab, tableName, null, null, identityName, subPropData, pi.PropertyType, prefix + prop.Name + "_", SubTable, columns, errorList, savedColumnsWithConstraints);
                        } else
                            throw new InternalError("Unknown property type {2} used in class {0}, property {1}", tpContainer.FullName, prop.Name, pi.PropertyType.FullName);
                    }
                }
            }
            return hasSubTable;
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
        private Microsoft.SqlServer.Management.Smo.DataType GetDataType(PropertyInfo pi) {
            Type tp = pi.PropertyType;
            if (tp == typeof(DateTime) || tp == typeof(DateTime?))
                return Microsoft.SqlServer.Management.Smo.DataType.DateTime2(7);
            else if (tp == typeof(TimeSpan) || tp == typeof(TimeSpan?))
                return Microsoft.SqlServer.Management.Smo.DataType.BigInt;
            else if (tp == typeof(decimal) || tp == typeof(decimal?))
                return Microsoft.SqlServer.Management.Smo.DataType.Money;
            else if (tp == typeof(bool) || tp == typeof(bool?))
                return Microsoft.SqlServer.Management.Smo.DataType.Bit;
            else if (tp == typeof(System.Guid) || tp == typeof(System.Guid?))
                return Microsoft.SqlServer.Management.Smo.DataType.UniqueIdentifier;
            else if (tp == typeof(Image))
                return Microsoft.SqlServer.Management.Smo.DataType.VarBinaryMax;
            else if (tp == typeof(int) || tp == typeof(int?))
                return Microsoft.SqlServer.Management.Smo.DataType.Int;
            else if (tp == typeof(long) || tp == typeof(long?))
                return Microsoft.SqlServer.Management.Smo.DataType.BigInt;
            else if (tp == typeof(Single) || tp == typeof(Single?))
                return Microsoft.SqlServer.Management.Smo.DataType.Float;
            else if (tp.IsEnum)
                return Microsoft.SqlServer.Management.Smo.DataType.Int;
            else if (tp == typeof(string)) {
                StringLengthAttribute attr = (StringLengthAttribute) pi.GetCustomAttribute(typeof(StringLengthAttribute));
                if (attr == null)
                    throw new InternalError("StringLength attribute missing for property {0} of type {1}", pi.Name, tp.FullName);
                int len = attr.MaximumLength;
                if (len == 0 || len >= 4000)
                    return Microsoft.SqlServer.Management.Smo.DataType.NVarCharMax;
                else
                    return Microsoft.SqlServer.Management.Smo.DataType.NVarChar(len);
            }
            throw new InternalError("Unsupported property type {0} for property {1}", tp.FullName, pi.Name);
        }

        // Index removal (for upgrades only)

        protected void RemoveIndexesIfNeeded(Database db) {
            if (!Package.MajorDataChange) return;
            if (DBsCompleted == null) DBsCompleted = new List<string>();
            if (DBsCompleted.Contains(db.Name)) return; // already done
            // do multiple passes until no more indexes available (we don't want to figure out the dependencies)
            int passes = 0;
            for (; ; ++passes) {
                int drop = 0;
                int failures = 0;
                foreach (Table table in db.Tables) {
                    int tableFailures = 0;
                    for (int i = table.ForeignKeys.Count; i > 0; --i) {
                        try {
                            table.ForeignKeys[i - 1].Drop();
                            ++drop;
                        } catch (Exception) { ++tableFailures; }
                    }
                    for (int i = table.Indexes.Count; i > 0; --i) {
                        try {
                            table.Indexes[i - 1].Drop();
                            ++drop;
                        } catch (Exception) { ++tableFailures; }
                    }
                    if (tableFailures == 0) {
                        foreach (Column column in table.Columns) {
                            if (column.DefaultConstraint != null)
                                column.DefaultConstraint.Drop();
                        }
                        table.Alter();
                    }
                    failures += tableFailures;
                }
                if (failures == 0)
                    break;// successfully removed everything
                if (drop == 0) {
                    throw new InternalError("No index/foreign keys could be dropped on the last pass in DB {0}", db.Name);
                }
            }
            DBsCompleted.Add(db.Name);
        }
        public void DropAllTables(Database db, string dbOwner) {
            // don't do any logging here - we might be deleting the tables needed for logging
            int maxTimes = 5;
            for (int time = maxTimes; time > 0 && db.Tables.Count > 0; --time) {
                List<Table> tables = (from Table t in db.Tables select t).ToList<Table>();
                foreach (Table table in tables) {
                    if (table.Schema == dbOwner) {
                        try {
                            table.Drop();
                        } catch (Exception) { }
                    }
                }
            }
            SQLCache.ClearCache();
        }
        public bool DropTable(Database db, string dbOwner, string tableName, List<string> errorList) {
            foreach (Table table in db.Tables) {
                if (table.Schema == dbOwner && table.Name == tableName) {
                    try {
                        table.Drop();
                        return true;
                    } catch (Exception exc) {
                        if (Logging) YetaWF.Core.Log.Logging.AddErrorLog("Couldn't drop table {0}", table.Name, exc);
                        errorList.Add(string.Format("Couldn't drop table {0}", table.Name));
                        while (exc != null && exc.Message != null) {
                            errorList.Add(exc.Message);
                            exc = exc.InnerException;
                        }
                        return false;
                    }
                }
            }
            errorList.Add(string.Format("Table {0} not found - can't be dropped", tableName));
            return false;
        }
        public bool DropSubTables(Database db, string dbOwner, string tableName, List<string> errorList) {
            bool status = true;
            string subtablePrefix = tableName + "_";
            Table[] tables = (from Table t in db.Tables select t).ToArray<Table>();
            foreach (Table table in tables) {
                if (table.Name.StartsWith(subtablePrefix))
                    if (!DropTable(db, dbOwner, table.Name, errorList))
                        status = false;
            }
            return status;
        }
    }
}