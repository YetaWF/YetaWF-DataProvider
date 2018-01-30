using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Reflection;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.SQL2 {

    public partial class SQLSimpleObject<KEYTYPE, OBJTYPE> : SQLSimpleObjectBase<KEYTYPE, object, OBJTYPE> {
        public SQLSimpleObject(Dictionary<string, object> options) : base(options) { }
    }
    public partial class SQLSimpleObjectBase<KEYTYPE, KEYTYPE2, OBJTYPE> : SQL2Base, IDataProvider<KEYTYPE, OBJTYPE>, ISQLTableInfo {
    
        public SQLSimpleObjectBase(Dictionary<string, object> options, bool HasKey2 = false) : base(options) {
            this.HasKey2 = HasKey2;
        }

        public bool HasKey2 { get; protected set; }
        public string Key1Name { get { return GetKey1Name(Dataset, ObjectSupport.GetPropertyData(typeof(OBJTYPE))); } }
        public string Key2Name { get { return GetKey2Name(Dataset, ObjectSupport.GetPropertyData(typeof(OBJTYPE))); } }
        public string IdentityName { get { return GetIdentityName(Dataset, ObjectSupport.GetPropertyData(typeof(OBJTYPE))); } }

        protected const int ChunkSize = 100;

        public OBJTYPE Get(KEYTYPE key, bool SpecificType = false) { //$$$remove specifictype?
            return Get(key, default(KEYTYPE2), SpecificType: SpecificType);
        }
        public OBJTYPE Get(KEYTYPE key, KEYTYPE2 key2) {
            return Get(key, default(KEYTYPE2), false);
        }
        public OBJTYPE Get(KEYTYPE key, KEYTYPE2 key2, bool SpecificType = false) {
            if (SpecificType) throw new InternalError("SpecificType not supported");

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string joins = null;// RFFU
            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            string calcProps = CalculatedProperties(typeof(OBJTYPE));
            string andKey2 = HasKey2 ? "AND " + sqlHelper.Expr(Key2Name, "=", key2) : null;

            List <PropertyData> propData = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
            string subTablesSelects = SubTablesSelects(Dataset, propData, typeof(OBJTYPE));

            string scriptMain = $@"
SELECT TOP 1 * -- result set
    {calcProps} 
FROM {fullTableName} WITH(NOLOCK) {joins}
WHERE {sqlHelper.Expr(Key1Name, "=", key)} {andKey2} {AndSiteIdentity}

{sqlHelper.DebugInfo}";

            string scriptWithSub = $@"
SELECT TOP 1 *
INTO #TEMPTABLE
FROM {fullTableName} WITH(NOLOCK) {joins}
WHERE {sqlHelper.Expr(Key1Name, "=", key)} {andKey2} {AndSiteIdentity}
;
SELECT * FROM #TEMPTABLE --- result set
;
DECLARE @MyCursor CURSOR;
DECLARE @ident int;

SET @MyCursor = CURSOR FOR
SELECT [{IdentityName}] FROM #TEMPTABLE

OPEN @MyCursor
FETCH NEXT FROM @MyCursor
INTO @ident
 
{subTablesSelects}

CLOSE @MyCursor ;
DEALLOCATE @MyCursor;
DROP TABLE #TEMPTABLE

{sqlHelper.DebugInfo}";

            string script = (string.IsNullOrWhiteSpace(subTablesSelects)) ? scriptMain : scriptWithSub;

            using (SqlDataReader reader = sqlHelper.ExecuteReader(script)) {
                if (!reader.Read()) return default(OBJTYPE);
                OBJTYPE obj = sqlHelper.CreateObject<OBJTYPE>(reader);
                if (!string.IsNullOrWhiteSpace(subTablesSelects)) {
                    ReadSubTables(sqlHelper, reader, Dataset, IdentityName, obj, propData, typeof(OBJTYPE));
                }
                return obj;
            }
        }

        public bool Add(OBJTYPE obj) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
            string columns = GetColumnList(propData, obj.GetType(), "", true, SiteSpecific: SiteIdentity > 0);
            string values = GetValueList(sqlHelper, Dataset, obj, propData, typeof(OBJTYPE), SiteSpecific: SiteIdentity > 0);

            string subTablesInserts = SubTablesInserts(sqlHelper, Dataset, obj, propData, typeof(OBJTYPE));

            string scriptMain = $@"
INSERT INTO {fullTableName} ({columns})
VALUES ({values})
;
SELECT @@IDENTITY -- result set

{sqlHelper.DebugInfo}";

            string scriptWithSub = $@"
INSERT INTO {fullTableName} ({columns})
VALUES ({values})
;
SELECT @@IDENTITY -- result set
;
DECLARE @__IDENTITY int = @@IDENTITY
;
{subTablesInserts}

{sqlHelper.DebugInfo}";

            string script = (string.IsNullOrWhiteSpace(subTablesInserts)) ? scriptMain : scriptWithSub;

            int identity = 0;
            try {
                object val = sqlHelper.ExecuteScalar(script);
                identity = Convert.ToInt32(val);
            } catch (Exception exc) {
                SqlException sqlExc = exc as SqlException;
                if (sqlExc != null && sqlExc.Number == 2627) // already exists
                    return false;
                throw new InternalError("Add failed for type {0} - {1}", typeof(OBJTYPE).FullName, exc.Message);
            }

            if (IdentityName != IdentityColumn) {
                PropertyInfo piIdent = ObjectSupport.GetProperty(typeof(OBJTYPE), IdentityName);
                if (piIdent.PropertyType != typeof(int)) throw new InternalError($"Object identities must be of type int in {typeof(OBJTYPE).FullName}");
                piIdent.SetValue(obj, identity);
            }
            return true;
        }

        public UpdateStatusEnum Update(KEYTYPE origKey, KEYTYPE newKey, OBJTYPE obj) {
            return Update(origKey, default(KEYTYPE2), newKey, default(KEYTYPE2), obj);
        }

        public UpdateStatusEnum Update(KEYTYPE origKey, KEYTYPE2 origKey2, KEYTYPE newKey, KEYTYPE2 newKey2, OBJTYPE obj) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
            string setColumns = SetColumns(sqlHelper, Dataset, IdentityName, propData, obj, typeof(OBJTYPE));
            string andKey2 = HasKey2 ? "AND " + sqlHelper.Expr(Key2Name, "=", origKey2) : null;

            string subTablesUpdates = SubTablesUpdates(sqlHelper, Dataset, obj, propData, typeof(OBJTYPE));

            string scriptMain = $@"
UPDATE {fullTableName} 
SET {setColumns}
WHERE {sqlHelper.Expr(Key1Name, "=", origKey)} {andKey2} {AndSiteIdentity}
;
SELECT @@ROWCOUNT --- result set

{sqlHelper.DebugInfo}";

            string scriptWithSub = $@"
DECLARE @__IDENTITY int;
SELECT @__IDENTITY = [{IdentityName}] FROM {fullTableName} 
WHERE {sqlHelper.Expr(Key1Name, "=", origKey)} {andKey2} {AndSiteIdentity}

UPDATE {fullTableName} 
SET {setColumns}
WHERE [{IdentityName}] = @__IDENTITY
;
SELECT @@ROWCOUNT --- result set

{subTablesUpdates}

{sqlHelper.DebugInfo}";

            string script = (string.IsNullOrWhiteSpace(subTablesUpdates)) ? scriptMain : scriptWithSub;

            try {
                object val = sqlHelper.ExecuteScalar(script);
                int changed = Convert.ToInt32(val);
                if (changed == 0)
                    return UpdateStatusEnum.RecordDeleted;
                if (changed > 1)
                    throw new InternalError($"Update failed - {changed} records updated");
            } catch (Exception exc) {
                if (!newKey.Equals(origKey)) {
                    SqlException sqlExc = exc as SqlException;
                    if (sqlExc != null && sqlExc.Number == 2627) {
                        // duplicate key violation, meaning the new key already exists
                        return UpdateStatusEnum.NewKeyExists;
                    }
                }
                throw new InternalError($"Update failed for type {typeof(OBJTYPE).FullName} - {exc.Message}");
            }
            return UpdateStatusEnum.OK;
        }

        public bool Remove(KEYTYPE key) {
            return Remove(key, default(KEYTYPE2));
        }
        public bool Remove(KEYTYPE key, KEYTYPE2 key2) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            string andKey2 = HasKey2 ? "AND " + sqlHelper.Expr(Key2Name, "=", key2) : null;

            List<PropertyData> propData = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
            string subTablesDeletes = SubTablesDeletes(fullTableName, propData, typeof(OBJTYPE));

            string scriptMain = $@"
DELETE
FROM {fullTableName} 
WHERE {sqlHelper.Expr(Key1Name, "=", key)} {andKey2} {AndSiteIdentity}
;
SELECT @@ROWCOUNT --- result set

{sqlHelper.DebugInfo}";

            string scriptWithSub = $@"
DECLARE @ident int;
SELECT @ident = [{IdentityName}] FROM {fullTableName} 
WHERE {sqlHelper.Expr(Key1Name, "=", key)} {andKey2} {AndSiteIdentity}

DELETE
FROM {fullTableName} 
WHERE [{IdentityName}] = @ident
;
SELECT @@ROWCOUNT --- result set

{subTablesDeletes}

{sqlHelper.DebugInfo}";

            string script = (string.IsNullOrWhiteSpace(subTablesDeletes)) ? scriptMain : scriptWithSub;

            object val = sqlHelper.ExecuteScalar(script);
            int deleted = Convert.ToInt32(val);
            if (deleted > 1)
                throw new InternalError($"More than 1 record deleted by {nameof(Remove)} method");
            return deleted > 0;
        }

        public OBJTYPE GetOneRecord(List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            filters = NormalizeFilter(typeof(OBJTYPE), filters);
            int total;
            OBJTYPE obj = GetMainTableRecords(0, 1, null, filters, out total, Joins: Joins).FirstOrDefault();
            return obj;
        }

        public List<OBJTYPE> GetRecords(int skip, int take, List<DataProviderSortInfo> sorts, List<DataProviderFilterInfo> filters, out int total, List<JoinData> Joins = null, bool SpecificType = false) {
            if (SpecificType) throw new InternalError("SpecificType not supported");

            filters = NormalizeFilter(typeof(OBJTYPE), filters);
            sorts = NormalizeSort(typeof(OBJTYPE), sorts);
            return GetMainTableRecords(skip, take, sorts, filters, out total, Joins: Joins);
        }

        public int RemoveRecords(List<DataProviderFilterInfo> filters) {
            filters = NormalizeFilter(typeof(OBJTYPE), filters);

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
            string filter = MakeFilter(sqlHelper, filters);

            string subTablesDeletes = SubTablesDeletes(Dataset, propData, typeof(OBJTYPE));

            string scriptMain = $@"
DELETE
FROM {fullTableName} 
{filter} 

{sqlHelper.DebugInfo}";

            string scriptWithSub = $@"
SELECT [{IdentityName}]
INTO #TEMPTABLE
FROM {fullTableName} WITH(NOLOCK) 
{filter} 
;
DELETE
FROM {fullTableName} WITH(NOLOCK) 
{filter} 
;
SELECT @@ROWCOUNT --- result set
;
SELECT * FROM #TEMPTABLE --- result set
;
DECLARE @MyCursor CURSOR;
DECLARE @ident int;

SET @MyCursor = CURSOR FOR
SELECT [{IdentityName}] FROM #TEMPTABLE

OPEN @MyCursor
FETCH NEXT FROM @MyCursor
INTO @ident
 
WHILE @@FETCH_STATUS = 0
BEGIN
	{subTablesDeletes}
    FETCH NEXT FROM @MyCursor INTO @ident
END; 

CLOSE @MyCursor ;
DEALLOCATE @MyCursor;
DROP TABLE #TEMPTABLE

{sqlHelper.DebugInfo}";

            string script = (string.IsNullOrWhiteSpace(subTablesDeletes)) ? scriptMain : scriptWithSub;

            object val = sqlHelper.ExecuteScalar(script);
            int deleted = Convert.ToInt32(val);
            return deleted;
        }

        protected List<OBJTYPE> GetMainTableRecords(int skip, int take, List<DataProviderSortInfo> sorts, List<DataProviderFilterInfo> filters, out int total, List<JoinData> Joins = null) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            total = 0;
            // get total # of records (only if a subset is requested)
            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
            Dictionary<string, string> visibleColumns = GetVisibleColumns(Database, Dbo, Dataset, typeof(OBJTYPE), Joins);
            string columnList = MakeColumnList(sqlHelper, visibleColumns, Joins);
            string joins = MakeJoins(sqlHelper, Joins);
            string filter = MakeFilter(sqlHelper, filters, visibleColumns);
            string calcProps = CalculatedProperties(typeof(OBJTYPE));
            string selectCount = null;
            if (skip != 0 || take != 0) {
                total = 0;
                SQLBuilder sb = new SQL2.SQLBuilder();
                sb.Add($"SELECT COUNT(*) FROM {fullTableName} WITH(NOLOCK) {joins} {filter} ");
                selectCount = sb.ToString();
            }

            string orderBy = null;
            {
                SQLBuilder sb = new SQL2.SQLBuilder();
                if (sorts == null || sorts.Count == 0)
                    sorts = new List<DataProviderSortInfo> { new DataProviderSortInfo { Field = Key1Name, Order = DataProviderSortInfo.SortDirection.Ascending } };
                sb.AddOrderBy(visibleColumns, sorts, skip, take);
                orderBy = sb.ToString();
            }

            string subTablesSelects = SubTablesSelects(Dataset, propData, typeof(OBJTYPE));

            string scriptMain = $@"
{selectCount} --- result set
SELECT {columnList} --- result set
    {calcProps} 
FROM {fullTableName} WITH(NOLOCK) 
{joins} 
{filter} 
{orderBy}

{sqlHelper.DebugInfo}";


            string scriptWithSub = $@"
{selectCount} --- result set
SELECT {columnList}
INTO #TEMPTABLE
FROM {fullTableName} WITH(NOLOCK) 
{joins} 
{filter} 
{orderBy}
;
SELECT * FROM #TEMPTABLE --- result set
;
DECLARE @MyCursor CURSOR;
DECLARE @ident int;

SET @MyCursor = CURSOR FOR
SELECT [{IdentityName}] FROM #TEMPTABLE

OPEN @MyCursor
FETCH NEXT FROM @MyCursor
INTO @ident
 
WHILE @@FETCH_STATUS = 0
BEGIN
	{subTablesSelects}
END; 

CLOSE @MyCursor ;
DEALLOCATE @MyCursor;
DROP TABLE #TEMPTABLE

{sqlHelper.DebugInfo}";

            string script = (string.IsNullOrWhiteSpace(subTablesSelects)) ? scriptMain : scriptWithSub;

            List<OBJTYPE> list = new List<OBJTYPE>();
            using (SqlDataReader reader = sqlHelper.ExecuteReader(script)) {
                if (skip != 0 || take != 0) {
                    if (!reader.Read()) throw new InternalError("Expected # of records");
                    total = reader.GetInt32(0);
                    if (!reader.NextResult()) throw new InternalError("Expected next result set (main table)");
                }
                while (reader.Read())
                    list.Add(sqlHelper.CreateObject<OBJTYPE>(reader));
                if (!string.IsNullOrWhiteSpace(subTablesSelects)) {
                    foreach (var obj in list) {
                        ReadSubTables(sqlHelper, reader, Dataset, IdentityName, obj, propData, typeof(OBJTYPE));
                    }
                }
                if (skip == 0 && take == 0)
                    total = list.Count;
                return list;
            }
        }

        public class SubTableInfo {
            public string Name { get; set; }
            public Type Type { get; set; }
            public PropertyInfo PropInfo { get; set; } // the container's property that hold this subtable
        }

        // TODO: Could add caching
        protected List<SubTableInfo> GetSubTables(string tableName, List<PropertyData> propData) {
            List<SubTableInfo> list = new List<SubTableInfo>();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        ; // nothing
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        ; // nothing
                    } else if (pi.PropertyType == typeof(Image)) {
                        ; // nothing
                    } else if (pi.PropertyType == typeof(TimeSpan)) {
                        ; // nothing
                    } else if (TryGetDataType(pi.PropertyType)) {
                        ; // nothing
                    } else if (pi.PropertyType.IsClass && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // enumerated type -> subtable
                        Type subType = pi.PropertyType.GetInterfaces().Where(t => t.IsGenericType == true && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                                .Select(t => t.GetGenericArguments()[0]).FirstOrDefault();
                        string subTableName = SQLBuilder.BuildFullTableName(tableName + "_" + pi.Name);
                        list.Add(new SubTableInfo {
                            Name = subTableName,
                            Type = subType,
                            PropInfo = pi,
                        });
                    }
                }
            }
            return list;
        }

        protected string SubTablesSelects(string tableName, List<PropertyData> propData, Type tpContainer) {
            SQLBuilder sb = new SQL2.SQLBuilder();
            List<SubTableInfo> subTables = GetSubTables(tableName, propData);
            if (subTables.Count > 0) {
                foreach (SubTableInfo subTable in subTables) {
                    sb.Add($@"
    SELECT * FROM {SQLBuilder.BuildFullTableName(Database, Dbo, subTable.Name)} WHERE {SQLBuilder.BuildFullColumnName(subTable.Name, SubTableKeyColumn)} = @ident ; --- result set
");
                }
                sb.Add(@"
    FETCH NEXT FROM @MyCursor INTO @ident
    ;
");
            }
            return sb.ToString();
        }

        protected void ReadSubTables(SQLHelper sqlHelper, SqlDataReader reader, string tableName, string identityName, OBJTYPE container, List<PropertyData> propData, Type tpContainer) {
            List<SubTableInfo> subTables = GetSubTables(tableName, propData);
            foreach (SubTableInfo subTable in subTables) {
                object subContainer = subTable.PropInfo.GetValue(container);
                if (subContainer == null) throw new InternalError($"{nameof(ReadSubTables)} encountered a enumeration property that is null");

                // find the Add method for the collection so we can add each item as its read
                MethodInfo addMethod = subTable.PropInfo.PropertyType.GetMethod("Add", new Type[] { subTable.Type });
                if (addMethod == null) throw new InternalError($"{nameof(ReadSubTables)} encountered a enumeration property that doesn't have an Add method");

                if (!reader.NextResult()) throw new InternalError("Expected next result set (subtable)");
                while (reader.Read()) {
                    object obj = sqlHelper.CreateObject(reader, subTable.Type);
                    addMethod.Invoke(subContainer, new object[] { obj });
                }
            }
        }

        protected string SubTablesInserts(SQLHelper sqlHelper, string tableName, object container, List<PropertyData> propData, Type tpContainer) {
            SQLBuilder sb = new SQL2.SQLBuilder();
            List<SubTableInfo> subTables = GetSubTables(tableName, propData);
            foreach (SubTableInfo subTable in subTables) {
                List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                IEnumerable ienum = (IEnumerable)subTable.PropInfo.GetValue(container);
                foreach (var obj in ienum) {
                    string columns = GetColumnList(subPropData, subTable.Type, "", false, SubTable: true);
                    string values = GetValueList(sqlHelper, Dataset, obj, subPropData, subTable.Type, "", false, SubTable: true);
                    sb.Add($@"
    INSERT INTO {subTable.Name} ({columns})
    VALUES ({values}) ;
");
                }
            }
            return sb.ToString();
        }
        protected string SubTablesUpdates(SQLHelper sqlHelper, string tableName, object container, List<PropertyData> propData, Type tpContainer) {
            SQLBuilder sb = new SQL2.SQLBuilder();
            List<SubTableInfo> subTables = GetSubTables(tableName, propData);
            foreach (SubTableInfo subTable in subTables) {
                sb.Add($@"
    DELETE FROM {subTable.Name} WHERE {SQL2Base.SubTableKeyColumn} = @__IDENTITY ;
");
                List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                IEnumerable ienum = (IEnumerable)subTable.PropInfo.GetValue(container);
                foreach (var obj in ienum) {
                    string columns = GetColumnList(subPropData, subTable.Type, "", false, SubTable: true);
                    string values = GetValueList(sqlHelper, Dataset, obj, subPropData, subTable.Type, "", false, SubTable: true);
                    sb.Add($@"
    INSERT INTO {subTable.Name} 
        ({columns})
        VALUES ({values}) ;
");
                }
            }
            return sb.ToString();
        }

        protected string SubTablesDeletes(string tableName, List<PropertyData> propData, Type tpContainer) {
            SQLBuilder sb = new SQL2.SQLBuilder();
            List<SubTableInfo> subTables = GetSubTables(tableName, propData);
            foreach (SubTableInfo subTable in subTables) {
                sb.Add($@"
    DELETE FROM {SQLBuilder.BuildFullTableName(Database, Dbo, subTable.Name)} WHERE {SQLBuilder.BuildFullColumnName(subTable.Name, SubTableKeyColumn)} = @ident ;
");
            }
            return sb.ToString();
        }

        // IINSTALLMODEL
        // IINSTALLMODEL
        // IINSTALLMODEL

        public bool IsInstalled() {
            return SQLCache.HasTable(Conn, Database, Dataset);
        }

        public bool InstallModel(List<string> errorList) {
            bool success = false;
            Database db = GetDatabase();
            List<string> columns = new List<string>();
            SQLCreate sqlCreate = new SQLCreate(Languages, IdentitySeed, Logging);
            success = sqlCreate.CreateTable(db, Dbo, Dataset, Key1Name, null, IdentityName, ObjectSupport.GetPropertyData(typeof(OBJTYPE)), typeof(OBJTYPE), errorList, columns,
                SiteSpecific: SiteIdentity > 0,
                TopMost: true);
            SQLCache.ClearCache();
            return success;
        }

        public bool UninstallModel(List<string> errorList) {
            try {
                Database db = GetDatabase();
                SQLCreate sqlCreate = new SQLCreate(Languages, IdentitySeed, Logging);
                //$$$ DropSubTables is questionable because we have models that use the package name and other models use packagename_xxxx
                sqlCreate.DropSubTables(db, Dbo, Dataset, errorList);
                sqlCreate.DropTable(db, Dbo, Dataset, errorList);
                return true;
            } catch (Exception exc) {
                errorList.Add(string.Format("{0}: {1}", typeof(OBJTYPE).FullName, exc.Message));
                return false;
            } finally {
                SQLCache.ClearCache();
            }
        }

        public void AddSiteData() { }
        public void RemoveSiteData() { // remove site-specific data
            if (SiteIdentity > 0) {
                string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
                SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                string script = $@"
DELETE FROM {fullTableName} {AndSiteIdentity}";
//$$$ delete subtable data
                sqlHelper.ExecuteScalar(script);
            }
        }

        public void ImportChunk(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            if (SiteIdentity > 0 || YetaWFManager.Manager.ImportChunksNonSiteSpecifics) {
                SerializableList<OBJTYPE> serList = (SerializableList<OBJTYPE>)obj;
                int total = serList.Count();
                if (total > 0) {
                    for (int processed = 0; processed < total; ++processed) {
                        OBJTYPE item = serList[processed];
                        if (!Add(item))
                            throw new InternalError("Add failed - item already exists");
                    }
                }
            }
        }

        public bool ExportChunk(int chunk, SerializableList<SerializableFile> fileList, out object obj, bool SpecificType = false) {
            if (SpecificType) throw new InternalError("SpecificType not supported");

            List<DataProviderSortInfo> sorts = new List<DataProviderSortInfo> { new DataProviderSortInfo { Field = Key1Name, Order = DataProviderSortInfo.SortDirection.Ascending } };
            int total;
            List<OBJTYPE> list = GetRecords(chunk * ChunkSize, ChunkSize, sorts, null, out total);
            obj = new SerializableList<OBJTYPE>(list);

            int count = list.Count();
            if (count == 0)
                obj = null;
            return (count >= ChunkSize);
        }
    }
}
