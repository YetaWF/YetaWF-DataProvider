/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.SQL {

    public partial class SQLSimpleObject<KEYTYPE, OBJTYPE> : SQLSimpleObjectBase<KEYTYPE, object, OBJTYPE> {
        public SQLSimpleObject(Dictionary<string, object> options) : base(options) { }
    }
    public partial class SQLSimpleObjectBase<KEYTYPE, KEYTYPE2, OBJTYPE> : SQLBase, IDataProvider<KEYTYPE, OBJTYPE>, ISQLTableInfo {

        public SQLSimpleObjectBase(Dictionary<string, object> options, bool HasKey2 = false) : base(options) {
            this.HasKey2 = HasKey2;
        }

        public bool HasKey2 { get; protected set; }
        public string Key1Name { get { return GetKey1Name(Dataset, GetPropertyData()); } }
        public string Key2Name { get { return GetKey2Name(Dataset, GetPropertyData()); } }
        public string IdentityName { get { return GetIdentityName(Dataset, GetPropertyData()); } }

        private string IdentityNameOrDefault {
            get {
                if (string.IsNullOrWhiteSpace(_identityOrDefault))
                    _identityOrDefault = GetIdentityName(Dataset, GetPropertyData());
                if (string.IsNullOrWhiteSpace(_identityOrDefault))
                    _identityOrDefault = SQLBase.IdentityColumn;
                return _identityOrDefault;
            }
        }
        private string _identityOrDefault;

        protected const int ChunkSize = 100;

        protected List<PropertyData> GetPropertyData() {
            if (_propertyData == null)
                _propertyData = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
            return _propertyData;
        }
        List<PropertyData> _propertyData;

        public Task<OBJTYPE> GetAsync(KEYTYPE key) {
            return GetAsync(key, default(KEYTYPE2));
        }
        public async Task<OBJTYPE> GetAsync(KEYTYPE key, KEYTYPE2 key2) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string joins = null;// RFFU
            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            string calcProps = await CalculatedPropertiesAsync(typeof(OBJTYPE));
            string andKey2 = HasKey2 ? "AND " + sqlHelper.Expr(Key2Name, "=", key2) : null;

            List<PropertyData> propData = GetPropertyData();
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
SELECT [{IdentityNameOrDefault}] FROM #TEMPTABLE

OPEN @MyCursor
FETCH NEXT FROM @MyCursor
INTO @ident
 
{subTablesSelects}

CLOSE @MyCursor ;
DEALLOCATE @MyCursor;
DROP TABLE #TEMPTABLE

{sqlHelper.DebugInfo}";

            string script = (string.IsNullOrWhiteSpace(subTablesSelects)) ? scriptMain : scriptWithSub;

            using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(script)) {
                if (! (YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) return default(OBJTYPE);
                OBJTYPE obj = sqlHelper.CreateObject<OBJTYPE>(reader);
                if (!string.IsNullOrWhiteSpace(subTablesSelects)) {
                    await ReadSubTablesAsync(sqlHelper, reader, Dataset, obj, propData, typeof(OBJTYPE));
                }
                return obj;
            }
        }

        public async Task<bool> AddAsync(OBJTYPE obj) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = GetPropertyData();
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
                if (HasIdentity(IdentityName)) {
                    object val = await sqlHelper.ExecuteScalarAsync(script);
                    identity = Convert.ToInt32(val);
                } else {
                    await sqlHelper.ExecuteNonQueryAsync(script);
                }
            } catch (Exception exc) {
                SqlException sqlExc = exc as SqlException;
                if (sqlExc != null && sqlExc.Number == 2627) // already exists
                    return false;
                throw new InternalError("Add failed for type {0} - {1}", typeof(OBJTYPE).FullName, exc.Message);
            }

            if (HasIdentity(IdentityName)) {
                PropertyInfo piIdent = ObjectSupport.GetProperty(typeof(OBJTYPE), IdentityName);
                if (piIdent.PropertyType != typeof(int)) throw new InternalError($"Object identities must be of type int in {typeof(OBJTYPE).FullName}");
                piIdent.SetValue(obj, identity);
            }
            return true;
        }

        public async Task<UpdateStatusEnum> UpdateAsync(KEYTYPE origKey, KEYTYPE newKey, OBJTYPE obj) {
            return await UpdateAsync(origKey, default(KEYTYPE2), newKey, default(KEYTYPE2), obj);
        }

        public async Task<UpdateStatusEnum> UpdateAsync(KEYTYPE origKey, KEYTYPE2 origKey2, KEYTYPE newKey, KEYTYPE2 newKey2, OBJTYPE obj) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = GetPropertyData();
            string setColumns = SetColumns(sqlHelper, Dataset, propData, obj, typeof(OBJTYPE));
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
SELECT @__IDENTITY = [{IdentityNameOrDefault}] FROM {fullTableName} 
WHERE {sqlHelper.Expr(Key1Name, "=", origKey)} {andKey2} {AndSiteIdentity}

UPDATE {fullTableName} 
SET {setColumns}
WHERE [{IdentityNameOrDefault}] = @__IDENTITY
;
SELECT @@ROWCOUNT --- result set

{subTablesUpdates}

{sqlHelper.DebugInfo}";

            string script = (string.IsNullOrWhiteSpace(subTablesUpdates)) ? scriptMain : scriptWithSub;

            try {
                object val = await sqlHelper.ExecuteScalarAsync(script);
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

        public async Task<bool> RemoveAsync(KEYTYPE key) {
            return await RemoveAsync(key, default(KEYTYPE2));
        }
        public async Task<bool> RemoveAsync(KEYTYPE key, KEYTYPE2 key2) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            string andKey2 = HasKey2 ? "AND " + sqlHelper.Expr(Key2Name, "=", key2) : null;

            List<PropertyData> propData = GetPropertyData();
            string subTablesDeletes = SubTablesDeletes(Dataset, propData, typeof(OBJTYPE));

            string scriptMain = $@"
DELETE
FROM {fullTableName} 
WHERE {sqlHelper.Expr(Key1Name, "=", key)} {andKey2} {AndSiteIdentity}
;
SELECT @@ROWCOUNT --- result set

{sqlHelper.DebugInfo}";

            string scriptWithSub = $@"
DECLARE @ident int;
SELECT @ident = [{IdentityNameOrDefault}] FROM {fullTableName} 
WHERE {sqlHelper.Expr(Key1Name, "=", key)} {andKey2} {AndSiteIdentity}

{subTablesDeletes}

DELETE
FROM {fullTableName} 
WHERE [{IdentityNameOrDefault}] = @ident
;
SELECT @@ROWCOUNT --- result set

{sqlHelper.DebugInfo}";

            string script = (string.IsNullOrWhiteSpace(subTablesDeletes)) ? scriptMain : scriptWithSub;

            object val = await sqlHelper.ExecuteScalarAsync(script);
            int deleted = Convert.ToInt32(val);
            if (deleted > 1)
                throw new InternalError($"More than 1 record deleted by {nameof(RemoveAsync)} method");
            return deleted > 0;
        }

        public async Task<OBJTYPE> GetOneRecordAsync(List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            filters = NormalizeFilter(typeof(OBJTYPE), filters);
            DataProviderGetRecords<OBJTYPE> recs = await GetMainTableRecordsAsync(0, 1, null, filters, Joins: Joins);
            return recs.Data.FirstOrDefault();
        }

        public async Task<DataProviderGetRecords<OBJTYPE>> GetRecordsAsync(int skip, int take, List<DataProviderSortInfo> sorts, List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            filters = NormalizeFilter(typeof(OBJTYPE), filters);
            sorts = NormalizeSort(typeof(OBJTYPE), sorts);
            return await GetMainTableRecordsAsync(skip, take, sorts, filters, Joins: Joins);
        }

        public async Task<int> RemoveRecordsAsync(List<DataProviderFilterInfo> filters) {
            filters = NormalizeFilter(typeof(OBJTYPE), filters);

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = GetPropertyData();
            string filter = MakeFilter(sqlHelper, filters);

            string subTablesDeletes = SubTablesDeletes(Dataset, propData, typeof(OBJTYPE));

            string scriptMain = $@"
DELETE
FROM {fullTableName} 
{filter} 

{sqlHelper.DebugInfo}";

            string scriptWithSub = $@"
SELECT [{IdentityNameOrDefault}]
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
SELECT [{IdentityNameOrDefault}] FROM #TEMPTABLE

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

            object val = await sqlHelper.ExecuteScalarAsync(script);
            int deleted = Convert.ToInt32(val);
            return deleted;
        }

        protected async Task<DataProviderGetRecords<OBJTYPE>> GetMainTableRecordsAsync(int skip, int take, List<DataProviderSortInfo> sorts, List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            DataProviderGetRecords<OBJTYPE> recs = new DataProviderGetRecords<OBJTYPE>(); 

            // get total # of records (only if a subset is requested)
            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = GetPropertyData();
            Dictionary<string, string> visibleColumns = GetVisibleColumns(Database, Dbo, Dataset, typeof(OBJTYPE), Joins);
            string columnList = MakeColumnList(sqlHelper, visibleColumns, Joins);
            string joins = MakeJoins(sqlHelper, Joins);
            string filter = MakeFilter(sqlHelper, filters, visibleColumns);
            string calcProps = await CalculatedPropertiesAsync(typeof(OBJTYPE));
            string selectCount = null;
            if (skip != 0 || take != 0) {
                SQLBuilder sb = new SQLBuilder();
                sb.Add($"SELECT COUNT(*) FROM {fullTableName} WITH(NOLOCK) {joins} {filter} ");
                selectCount = sb.ToString();
            }

            string orderBy = null;
            {
                SQLBuilder sb = new SQLBuilder();
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
SELECT [{IdentityNameOrDefault}] FROM #TEMPTABLE

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

            using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(script)) {
                if (skip != 0 || take != 0) {
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) throw new InternalError("Expected # of records");
                    recs.Total = reader.GetInt32(0);
                    if (!(YetaWFManager.IsSync() ? reader.NextResult() : await reader.NextResultAsync())) throw new InternalError("Expected next result set (main table)");
                }
                while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync()))
                    recs.Data.Add(sqlHelper.CreateObject<OBJTYPE>(reader));
                if (!string.IsNullOrWhiteSpace(subTablesSelects)) {
                    foreach (var obj in recs.Data) {
                        await ReadSubTablesAsync(sqlHelper, reader, Dataset, obj, propData, typeof(OBJTYPE));
                    }
                }
                if (skip == 0 && take == 0)
                    recs.Total = recs.Data.Count;
                return recs;
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
            SQLBuilder sb = new SQLBuilder();
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

        protected async Task ReadSubTablesAsync(SQLHelper sqlHelper, SqlDataReader reader, string tableName, OBJTYPE container, List<PropertyData> propData, Type tpContainer) {
            List<SubTableInfo> subTables = GetSubTables(tableName, propData);
            foreach (SubTableInfo subTable in subTables) {
                object subContainer = subTable.PropInfo.GetValue(container);
                if (subContainer == null) throw new InternalError($"{nameof(ReadSubTablesAsync)} encountered a enumeration property that is null");

                // find the Add method for the collection so we can add each item as its read
                MethodInfo addMethod = subTable.PropInfo.PropertyType.GetMethod("Add", new Type[] { subTable.Type });
                if (addMethod == null) throw new InternalError($"{nameof(ReadSubTablesAsync)} encountered a enumeration property that doesn't have an Add method");

                if (!(YetaWFManager.IsSync() ? reader.NextResult() : await reader.NextResultAsync())) throw new InternalError("Expected next result set (subtable)");
                while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) {
                    object obj = sqlHelper.CreateObject(reader, subTable.Type);
                    addMethod.Invoke(subContainer, new object[] { obj });
                }
            }
        }

        protected string SubTablesInserts(SQLHelper sqlHelper, string tableName, object container, List<PropertyData> propData, Type tpContainer) {
            SQLBuilder sb = new SQLBuilder();
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
            SQLBuilder sb = new SQLBuilder();
            List<SubTableInfo> subTables = GetSubTables(tableName, propData);
            foreach (SubTableInfo subTable in subTables) {
                sb.Add($@"
    DELETE FROM {subTable.Name} WHERE {SQLBase.SubTableKeyColumn} = @__IDENTITY ;
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
            SQLBuilder sb = new SQLBuilder();
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

        public Task<bool> IsInstalledAsync() {
            return Task.FromResult(SQLCache.HasTable(Conn, Database, Dataset));
        }

        public Task<bool> InstallModelAsync(List<string> errorList) {
            bool success = false;
            Database db = GetDatabase();
            List<string> columns = new List<string>();
            SQLCreate sqlCreate = new SQLCreate(Languages, IdentitySeed, Logging);
            //TODO: could asyncify but probably not worth it as this is used during install/startup only
            success = sqlCreate.CreateTable(db, Dbo, Dataset, Key1Name, HasKey2 ? Key2Name : null, IdentityName, GetPropertyData(), typeof(OBJTYPE), errorList, columns,
                SiteSpecific: SiteIdentity > 0,
                TopMost: true);
            SQLCache.ClearCache();
            return Task.FromResult(success);
        }

        public Task<bool> UninstallModelAsync(List<string> errorList) {
            try {
                Database db = GetDatabase();
                SQLCreate sqlCreate = new SQLCreate(Languages, IdentitySeed, Logging);
                List<PropertyData> propData = GetPropertyData();
                List<SubTableInfo> subTables = GetSubTables(Dataset, propData);
                foreach (SubTableInfo subTable in subTables) {
                    //TODO: could asyncify but probably not worth it as this is used during install/startup only
                    sqlCreate.DropTable(db, Dbo, subTable.Name, errorList);
                }
                sqlCreate.DropTable(db, Dbo, Dataset, errorList);
                return Task.FromResult(true);
            } catch (Exception exc) {
                errorList.Add(string.Format("{0}: {1}", typeof(OBJTYPE).FullName, exc.Message));
                return Task.FromResult(false);
            } finally {
                SQLCache.ClearCache();
            }
        }

        public Task AddSiteDataAsync() { return Task.CompletedTask; }
        public async Task RemoveSiteDataAsync() { // remove site-specific data
            if (SiteIdentity > 0) {
                string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
                SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                SQLBuilder sb = new SQLBuilder();
                sb.Add($@"
DELETE FROM {fullTableName} WHERE [{SiteColumn}] = {SiteIdentity}
;
");
                // subtable data is removed by delete cascade
                await sqlHelper.ExecuteScalarAsync(sb.ToString());
            }
        }

        public async Task ImportChunkAsync(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            if (SiteIdentity > 0 || YetaWFManager.Manager.ImportChunksNonSiteSpecifics) {
                SerializableList<OBJTYPE> serList = (SerializableList<OBJTYPE>)obj;
                int total = serList.Count();
                if (total > 0) {
                    for (int processed = 0; processed < total; ++processed) {
                        OBJTYPE item = serList[processed];
                        if (!await AddAsync(item))
                            throw new InternalError("Add failed - item already exists");
                    }
                }
            }
        }

        public async Task<DataProviderExportChunk> ExportChunkAsync(int chunk, SerializableList<SerializableFile> fileList) {
            List<DataProviderSortInfo> sorts = new List<DataProviderSortInfo> { new DataProviderSortInfo { Field = Key1Name, Order = DataProviderSortInfo.SortDirection.Ascending } };

            DataProviderGetRecords<OBJTYPE> recs = await GetRecordsAsync(chunk * ChunkSize, ChunkSize, sorts, null);

            int count = recs.Data.Count();
            if (count == 0) {
                return new DataProviderExportChunk {
                    ObjectList = null,
                    More = false,
                };
            } else {
                return new DataProviderExportChunk {
                    ObjectList = new SerializableList<OBJTYPE>(recs.Data),
                    More = count >= ChunkSize,
                };
            }
        }
    }
}
