/* Copyright © 2019 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Models;
using YetaWF.Core.Modules;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.SQL {

    public partial class SQLModuleObject<KEY, OBJTYPE> : SQLSimpleObject<KEY, OBJTYPE>, IDataProvider<KEY, OBJTYPE> {

        public SQLModuleObject(Dictionary<string, object> options) : base(options) {
            if (typeof(KEY) != typeof(Guid)) throw new InternalError("Only Guid is supported as Key");
            BaseDataset = ModuleDefinition.BaseFolderName;
            if (typeof(OBJTYPE) != typeof(ModuleDefinition))
                Dataset = ModuleDefinition.BaseFolderName + "_" + Package.AreaName + "_" + typeof(OBJTYPE).Name;
        }

        public string BaseDataset { get; protected set; }

        public new async Task<OBJTYPE> GetAsync(KEY key) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullBaseTableName = SQLBuilder.GetTable(Database, Dbo, BaseDataset);

            if (Dataset == BaseDataset) {
                // we're reading the base and have to find the derived table

                string script = $@"
DECLARE @Table nvarchar(80);
DECLARE @Type nvarchar(200);
DECLARE @Asm nvarchar(200);

SELECT TOP 1 @Table=[DerivedDataTableName], @Type=[DerivedDataType], @Asm=[DerivedAssemblyName]
FROM {fullBaseTableName} WITH(NOLOCK)
WHERE {sqlHelper.Expr(Key1Name, "=", key)} {AndSiteIdentity}

IF @@ROWCOUNT > 0
BEGIN

SELECT @Table, @Type, @Asm  --- result set
;

EXEC ('SELECT TOP 1 * FROM {fullBaseTableName} AS A WITH(NOLOCK)
        LEFT JOIN [' + @Table + '] AS B ON
        A.[{Key1Name}] = B.[{Key1Name}] AND A.[{SiteColumn}] = B.[{SiteColumn}]
        WHERE A.[{Key1Name}] = ''{key}'' AND A.[{SiteColumn}] = {SiteIdentity}')   --- result set

END

{sqlHelper.DebugInfo}";

                using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(script)) {
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) return default(OBJTYPE);
                    string derivedTableName = (string)reader[0];
                    string derivedDataType = (string)reader[1];
                    string derivedAssemblyName = (string)reader[2];
                    if (string.IsNullOrWhiteSpace(derivedTableName))
                        return default(OBJTYPE);
                    if (!(YetaWFManager.IsSync() ? reader.NextResult() : await reader.NextResultAsync()))
                        return default(OBJTYPE);
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) return default(OBJTYPE);
                    OBJTYPE obj = sqlHelper.CreateObject<OBJTYPE>(reader, derivedDataType, derivedAssemblyName);
                    return obj;
                }

            } else {
                // we're reading the derived table
                string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
                string scriptMain = $@"
SELECT TOP 1 * FROM {fullBaseTableName} AS A WITH(NOLOCK)
LEFT JOIN {fullTableName} AS B ON 
    A.[{Key1Name}] = B.[{Key1Name}] AND A.[{SiteColumn}] = B.[{SiteColumn}]
WHERE {sqlHelper.Expr($"A.[{Key1Name}]", " =", key)} AND A.[{SiteColumn}] = {SiteIdentity}

{sqlHelper.DebugInfo}";

                using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(scriptMain)) {
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) return default(OBJTYPE);
                    OBJTYPE obj = sqlHelper.CreateObject<OBJTYPE>(reader);
                    return obj;
                }
            }
        }

        public new async Task<bool> AddAsync(OBJTYPE obj) {
            if (Dataset == BaseDataset) throw new InternalError("Only derived types are supported");

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullBaseTableName = SQLBuilder.GetTable(Database, Dbo, BaseDataset);
            List<PropertyData> propBaseData = GetBasePropertyData();
            string baseColumns = GetColumnList(propBaseData, typeof(ModuleDefinition), "", true, SiteSpecific: SiteIdentity > 0, WithDerivedInfo: true);
            string baseValues = GetValueList(sqlHelper, Dataset, obj, propBaseData, typeof(ModuleDefinition), SiteSpecific: SiteIdentity > 0, DerivedType: typeof(OBJTYPE), DerivedTableName: Dataset);
            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = GetPropertyData();
            string columns = GetColumnList(propData, obj.GetType(), "", true, SiteSpecific: SiteIdentity > 0);
            string values = GetValueList(sqlHelper, Dataset, obj, propData, typeof(OBJTYPE), SiteSpecific: SiteIdentity > 0);

            string scriptMain = $@"
INSERT INTO {fullBaseTableName} ({baseColumns})
VALUES ({baseValues})
;
INSERT INTO {fullTableName} ({columns})
VALUES ({values})
;
{sqlHelper.DebugInfo}";

            int identity = 0;
            try {
                await sqlHelper.ExecuteNonQueryAsync(scriptMain);
            } catch (Exception exc) {
                SqlException sqlExc = exc as SqlException;
                if (sqlExc != null && sqlExc.Number == 2627) // already exists
                    return false;
                throw new InternalError("Add failed for type {0} - {1}", typeof(OBJTYPE).FullName, ErrorHandling.FormatExceptionMessage(exc));
            }

            if (HasIdentity(IdentityName)) {
                PropertyInfo piIdent = ObjectSupport.GetProperty(typeof(OBJTYPE), IdentityName);
                if (piIdent.PropertyType != typeof(int)) throw new InternalError($"Object identities must be of type int in {typeof(OBJTYPE).FullName}");
                piIdent.SetValue(obj, identity);
            }
            return true;
        }

        public new async Task<UpdateStatusEnum> UpdateAsync(KEY origKey, KEY newKey, OBJTYPE obj) {
            if (Dataset == BaseDataset) throw new InternalError("Only derived types are supported");

            if (!origKey.Equals(newKey)) throw new InternalError("Can't change key");

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullBaseTableName = SQLBuilder.GetTable(Database, Dbo, BaseDataset);
            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);

            List<PropertyData> propBaseData = GetBasePropertyData();
            string setBaseColumns = SetColumns(sqlHelper, Dataset, propBaseData, obj, typeof(ModuleDefinition));
            List<PropertyData> propData = GetPropertyData();
            string setColumns = SetColumns(sqlHelper, Dataset, propData, obj, typeof(OBJTYPE));

            string scriptMain = $@"
UPDATE {fullBaseTableName} 
SET {setBaseColumns}
WHERE {sqlHelper.Expr(Key1Name, "=", origKey)} {AndSiteIdentity}
;
SELECT @@ROWCOUNT --- result set
;
UPDATE {fullTableName}
SET {setColumns}
WHERE {sqlHelper.Expr(Key1Name, "=", origKey)} {AndSiteIdentity}
;
{sqlHelper.DebugInfo}";

            try {
                object val = await sqlHelper.ExecuteScalarAsync(scriptMain);
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
                throw new InternalError($"Update failed for type {typeof(OBJTYPE).FullName} - {ErrorHandling.FormatExceptionMessage(exc)}");
            }
            return UpdateStatusEnum.OK;
        }

        public new async Task<bool> RemoveAsync(KEY key) {
            if (Dataset != BaseDataset) throw new InternalError("Only base types are supported");

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullBaseTableName = SQLBuilder.GetTable(Database, Dbo, BaseDataset);

            List<PropertyData> propData = GetPropertyData();

            string scriptMain = $@"
SELECT *
INTO #BASETABLE
FROM {fullBaseTableName} WITH(NOLOCK)
WHERE {sqlHelper.Expr(Key1Name, "=", key)} {AndSiteIdentity}

SELECT @@ROWCOUNT --- result set
;

IF @@ROWCOUNT > 0
BEGIN

DECLARE @Table nvarchar(80);

SELECT @Table=[DerivedDataTableName] FROM #BASETABLE
;
EXEC ('DELETE FROM [' + @Table + '] B WHERE B.[{Key1Name}] = ''{key}'' {AndSiteIdentity}')
;
DELETE
FROM {fullBaseTableName}
WHERE {sqlHelper.Expr(Key1Name, "=", key)} {AndSiteIdentity}
;
END

DROP TABLE #BASETABLE

{sqlHelper.DebugInfo}
";
            object val = await sqlHelper.ExecuteScalarAsync(scriptMain);
            int deleted = Convert.ToInt32(val);
            if (deleted > 1)
                throw new InternalError($"More than 1 record deleted by {nameof(RemoveAsync)} method");
            return deleted > 0;
        }

        public new Task<OBJTYPE> GetOneRecordAsync(List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            throw new NotImplementedException();
        }

        public new async Task<DataProviderGetRecords<OBJTYPE>> GetRecordsAsync(int skip, int take, List<DataProviderSortInfo> sorts, List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            if (Dataset == BaseDataset) {
                // we're reading the base table
                return await base.GetRecordsAsync(skip, take, sorts, filters, Joins: Joins);
            } else {
                // an explicit type is requested
                SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

                DataProviderGetRecords<OBJTYPE> recs = new DataProviderGetRecords<OBJTYPE>();

                string fullBaseTableName = SQLBuilder.GetTable(Database, Dbo, BaseDataset);
                string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);

                // get total # of records (only if a subset is requested)
                string selectCount = null;
                if (skip != 0 || take != 0) {
                    SQLBuilder sb = new SQLBuilder();
                    sb.Add($@"

SELECT COUNT(*)
FROM {fullBaseTableName} WITH(NOLOCK)

WHERE {fullBaseTableName}.[DerivedDataTableName] = '{Dataset}' AND {fullBaseTableName}.[DerivedDataType] = '{typeof(OBJTYPE).FullName}'
 AND {fullBaseTableName}.[{SiteColumn}] = {SiteIdentity}
");

                    selectCount = sb.ToString();
                }

                string orderby = null;
                if (skip != 0 || take != 0)
                    orderby = $"ORDER BY [Name] ASC OFFSET {skip} ROWS FETCH NEXT {take} ROWS ONLY";


                string script = $@"
{selectCount} --- result set

SELECT *
FROM {fullBaseTableName} WITH(NOLOCK)

LEFT JOIN {fullTableName} ON 
    {fullBaseTableName}.[{Key1Name}] = {fullTableName}.[{Key1Name}] AND {fullBaseTableName}.[{SiteColumn}] = {fullTableName}.[{SiteColumn}]

WHERE {fullBaseTableName}.[DerivedDataTableName] = '{Dataset}' AND {fullBaseTableName}.[DerivedDataType] = '{typeof(OBJTYPE).FullName}'
 AND {fullBaseTableName}.[{SiteColumn}] = {SiteIdentity}
{orderby}

{sqlHelper.DebugInfo}
";

                using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(script)) {
                    if (skip != 0 || take != 0) {
                        if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) throw new InternalError("Expected # of records");
                        recs.Total = reader.GetInt32(0);
                        if (!(YetaWFManager.IsSync() ? reader.NextResult() : await reader.NextResultAsync())) throw new InternalError("Expected next result set (table)");
                    }
                    while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync()))
                        recs.Data.Add(sqlHelper.CreateObject<OBJTYPE>(reader));

                    if (skip == 0 && take == 0)
                        recs.Total = recs.Data.Count;
                    return recs;
                }
            }
        }

        public new Task<int> RemoveRecordsAsync(List<DataProviderFilterInfo> filters) {
            throw new NotImplementedException();
        }

        protected List<PropertyData> GetBasePropertyData() {
            if (_basePropertyData == null)
                _basePropertyData = ObjectSupport.GetPropertyData(typeof(ModuleDefinition));
            return _basePropertyData;
        }
        private static List<PropertyData> _basePropertyData;

        protected new List<PropertyData> GetPropertyData() {
            if (_propertyData == null) {
                List<PropertyData> propData = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
                // subtract all properties that are already defined in the base type
                List<PropertyData> basePropData = GetBasePropertyData();
                _propertyData = new List<PropertyData>();
                foreach (var p in propData) {
                    if (p.HasAttribute(Data_PrimaryKey.AttributeName)) {
                        // The primary key has to be present in both derived and base table because they're used as foreign key
                        _propertyData.Add(p);
                    } else {
                        var first = (from bp in basePropData where bp.Name == p.Name select p).FirstOrDefault();
                        if (first == null)
                            _propertyData.Add(p);
                    }
                }
            }
            return _propertyData;
        }
        List<PropertyData> _propertyData;

        // IINSTALLMODEL
        // IINSTALLMODEL
        // IINSTALLMODEL

        public new Task<bool> IsInstalledAsync() {
            if (!SQLCache.HasTable(Conn, ConnectionString, Database, BaseDataset))
                return Task.FromResult(false);
            if (Dataset != BaseDataset) {
                if (!SQLCache.HasTable(Conn, ConnectionString, Database, Dataset))
                    return Task.FromResult(false);
            }
            return Task.FromResult(true);
        }

        public new Task<bool> InstallModelAsync(List<string> errorList) {
            if (Dataset == BaseDataset) throw new InternalError("Base dataset is not supported");
            bool success = false;
            Database db = SQLCache.GetDatabase(Conn, ConnectionString);
            success = CreateTableWithBaseType(db, errorList);
            SQLCache.ClearCache();
            return Task.FromResult(success);
        }
        private bool CreateTableWithBaseType(Database db, List<string> errorList) {
            Type baseType = typeof(ModuleDefinition);
            List<string> columns = new List<string>();
            SQLCreate sqlCreate = new SQLCreate(Languages, IdentitySeed, Logging);
            if (!sqlCreate.CreateTable(db, Dbo, BaseDataset, Key1Name, null, IdentityName, GetBasePropertyData(), baseType, errorList, columns,
                    TopMost: true,
                    SiteSpecific: SiteIdentity > 0,
                    DerivedDataTableName: "DerivedDataTableName", DerivedDataTypeName: "DerivedDataType", DerivedAssemblyName: "DerivedAssemblyName"))
                return false;
            return sqlCreate.CreateTable(db, Dbo, Dataset, Key1Name, null, SQLBase.IdentityColumn, GetPropertyData(), typeof(OBJTYPE), errorList, columns,
                TopMost: true,
                SiteSpecific: SiteIdentity > 0,
                ForeignKeyTable: BaseDataset);
        }

        public new async Task<bool> UninstallModelAsync(List<string> errorList) {
            if (Dataset == BaseDataset) throw new InternalError("Base dataset is not supported");
            try {
                Database db = SQLCache.GetDatabase(Conn, ConnectionString);
                await DropTableWithBaseType(db, errorList);
                return true;
            } catch (Exception exc) {
                errorList.Add(string.Format("{0}: {1}", typeof(OBJTYPE).FullName, ErrorHandling.FormatExceptionMessage(exc)));
                return false;
            } finally {
                SQLCache.ClearCache();
            }
        }
        private async Task<bool> DropTableWithBaseType(Database db, List<string> errorList) {
            try {
                if (db.Tables.Contains(Dataset)) {
                    // Remove all records from the table (this removes the records in BaseTableName also)
                    SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                    SQLBuilder sb = new SQLBuilder();
                    sb.Add($@"
DELETE {BaseDataset} FROM {BaseDataset}
    INNER JOIN {Dataset} ON {BaseDataset}.[{Key1Name}] = {Dataset}.[{Key1Name}]
                    ");
                    await sqlHelper.ExecuteNonQueryAsync(sb.ToString());
                    // then drop the table
                    db.Tables[Dataset].Drop();
                }
                if (db.Tables.Contains(BaseDataset)) {
                    SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                    SQLBuilder sb = new SQLBuilder();
                    sb.Add($@"
SELECT COUNT(*) FROM  {BaseDataset}
");
                    object val = await sqlHelper.ExecuteScalarAsync(sb.ToString());
                    int count = Convert.ToInt32(val);
                    if (count == 0)
                        db.Tables[BaseDataset].Drop();
                }
            } catch (Exception exc) {
                if (Logging) YetaWF.Core.Log.Logging.AddErrorLog("Couldn't drop table", exc);
                errorList.Add(string.Format("Couldn't drop table - {0}.", ErrorHandling.FormatExceptionMessage(exc)));
                return false;
            }
            return true;
        }
        public new async Task RemoveSiteDataAsync() { // remove site-specific data
            if (Dataset == BaseDataset) throw new InternalError("Base dataset is not supported");
            if (SiteIdentity > 0) {
                SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                SQLBuilder sb = new SQLBuilder();
                sb.Add($@"
DELETE FROM {Dataset} WHERE [{SiteColumn}] = {SiteIdentity}
DELETE FROM {BaseDataset} WHERE [DerivedDataTableName] = '{Dataset}' AND [{SiteColumn}] = {SiteIdentity}
");
                await sqlHelper.ExecuteNonQueryAsync(sb.ToString());
            }
        }

        public new async Task ImportChunkAsync(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            if (Dataset == BaseDataset) throw new InternalError("Base dataset is not supported");
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

        public new async Task<DataProviderExportChunk> ExportChunkAsync(int chunk, SerializableList<SerializableFile> fileList) {
            if (Dataset == BaseDataset) throw new InternalError("Base dataset is not supported");

            List<DataProviderSortInfo> sorts = new List<DataProviderSortInfo> { new DataProviderSortInfo { Field = Key1Name, Order = DataProviderSortInfo.SortDirection.Ascending } };
            DataProviderGetRecords<OBJTYPE> list = await GetRecordsAsync(chunk * ChunkSize, ChunkSize, sorts, null);

            int count = list.Data.Count();
            if (count == 0) {
                return new DataProviderExportChunk {
                    ObjectList = null,
                    More = false,
                };
            } else {
                return new DataProviderExportChunk {
                    ObjectList = new SerializableList<OBJTYPE>(list.Data),
                    More = count >= ChunkSize,
                };
            }
        }
        public new async Task LocalizeModelAsync(string language, Func<string, bool> isHtml, Func<List<string>, Task<List<string>>> translateStringsAsync, Func<string, Task<string>> translateComplexStringAsync) {

            await LocalizeModelAsync(language, isHtml, translateStringsAsync, translateComplexStringAsync,
                async (int offset, int skip) => {
                    return await GetRecordsAsync(offset, skip, null, null);
                },
                async (OBJTYPE record, PropertyInfo pi, PropertyInfo pi2) => {
                    UpdateStatusEnum status;
                    KEY key1 = (KEY)pi.GetValue(record);
                    status = await UpdateAsync(key1, key1, record);
                    return status;
                });
        }
    }
}
