/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
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
using YetaWF.DataProvider.SQLGeneric;
#if MVC6
using Microsoft.Data.SqlClient;
#else
using System.Data.SqlClient;
#endif

namespace YetaWF.DataProvider.SQL {

    /// <summary>
    /// This class implements the base functionality to access the repository containing YetaWF modules.
    /// It is only used by the YetaWF.DataProvider.ModuleDefinition package and is not intended for application use.
    /// </summary>
    public class SQLModuleObject<KEY, OBJTYPE> : SQLSimpleObject<KEY, OBJTYPE>, IDataProvider<KEY, OBJTYPE> {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="options">A dictionary of options and optional parameters as provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.</param>
        /// <remarks>
        /// Data providers are instantiated when the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method is called, usually by an application data provider.
        /// </remarks>
        public SQLModuleObject(Dictionary<string, object> options) : base(options) {
            if (typeof(KEY) != typeof(Guid)) throw new InternalError("Only Guid is supported as Key");
            BaseDataset = ModuleDefinition.BaseFolderName;
            if (typeof(OBJTYPE) != typeof(ModuleDefinition))
                Dataset = ModuleDefinition.BaseFolderName + "_" + Package.AreaName + "_" + typeof(OBJTYPE).Name;
        }

        /// <summary>
        /// Defines the base dataset for base module definitions.
        /// </summary>
        /// <remarks>All modules classes are derived from YetaWF.Core.Modules.ModuleDefinition. The data of the base class (YetaWF.Core.Modules.ModuleDefinition) is stored in the
        /// data provider's dataset defined by BaseDataset. All data for the derived class is stored in the data provider's dataset defined by Dataset.</remarks>
        public string BaseDataset { get; protected set; }

        /// <summary>
        /// Retrieves one record from the database table that satisfies the specified primary key <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The primary key value.</param>
        /// <returns>Returns the record that satisfies the specified primary key. If no record exists null is returned.</returns>
        public new async Task<OBJTYPE> GetAsync(KEY key) {
            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            SQLBuilder sb = new SQLBuilder();

            string fullBaseTableName = sb.GetTable(Database, Dbo, BaseDataset);

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
                string fullTableName = sb.GetTable(Database, Dbo, Dataset);
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

        /// <summary>
        /// Adds a new record to the database table.
        /// </summary>
        /// <param name="obj">The record data.</param>
        /// <returns>Returns true if successful, false if a record with the primary (and secondary) key values already exists.
        ///
        /// For all other errors, an exception occurs.
        /// </returns>
        public new async Task<bool> AddAsync(OBJTYPE obj) {
            SQLBuilder sb = new SQLBuilder();
            await EnsureOpenAsync();
            if (Dataset == BaseDataset) throw new InternalError("Only derived types are supported");

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullBaseTableName = sb.GetTable(Database, Dbo, BaseDataset);
            List<PropertyData> propBaseData = GetBasePropertyData();
            string baseColumns = GetColumnList(propBaseData, typeof(ModuleDefinition), "", true, SiteSpecific: SiteIdentity > 0, WithDerivedInfo: true);
            string baseValues = GetValueList(sqlHelper, Dataset, obj, propBaseData, typeof(ModuleDefinition), SiteSpecific: SiteIdentity > 0, DerivedType: typeof(OBJTYPE), DerivedTableName: Dataset);
            string fullTableName = sb.GetTable(Database, Dbo, Dataset);
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

        /// <summary>
        /// Updates an existing record with the specified existing primary key <paramref name="origKey"/> in the database table.
        /// The primary key can be changed to the new value in <paramref name="newKey"/>.
        /// </summary>
        /// <param name="origKey">The original primary key value of the record.</param>
        /// <param name="newKey">The new primary key value of the record. This may be the same value as <paramref name="origKey"/>. </param>
        /// <param name="obj">The object being updated.</param>
        /// <returns>Returns a status indicator.</returns>
        public new async Task<UpdateStatusEnum> UpdateAsync(KEY origKey, KEY newKey, OBJTYPE obj) {
            SQLBuilder sb = new SQLBuilder();
            await EnsureOpenAsync();
            if (Dataset == BaseDataset) throw new InternalError("Only derived types are supported");

            if (!origKey.Equals(newKey)) throw new InternalError("Can't change key");

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullBaseTableName = sb.GetTable(Database, Dbo, BaseDataset);
            string fullTableName = sb.GetTable(Database, Dbo, Dataset);

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

        /// <summary>
        /// Removes an existing record with the specified primary key.
        /// </summary>
        /// <param name="key">The primary key value of the record to remove.</param>
        /// <returns>Returns true if the record was removed, or false if the record was not found. Other errors cause an exception.</returns>
        public new async Task<bool> RemoveAsync(KEY key) {
            SQLBuilder sb = new SQLBuilder();
            await EnsureOpenAsync();
            if (Dataset != BaseDataset) throw new InternalError("Only base types are supported");

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullBaseTableName = sb.GetTable(Database, Dbo, BaseDataset);

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

        /// <summary>
        /// Retrieves one record using filtering criteria.
        /// </summary>
        /// <param name="filters">A collection describing the filtering criteria.</param>
        /// <param name="Joins">A collection describing the dataset joins.</param>
        /// <returns>If more than one record match the filtering criteria, the first one is returned.
        /// If no record matches, null is returned.</returns>
        /// <remarks>
        /// This is not implemented as it is not required for module storage.
        /// </remarks>
        public new Task<OBJTYPE> GetOneRecordAsync(List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Retrieves a collection of records using filtering criteria with sorting, with support for paging.
        /// </summary>
        /// <param name="skip">The number of records to skip (paging support).</param>
        /// <param name="take">The number of records to retrieve (paging support). If more records are available they are dropped.</param>
        /// <param name="sorts">A collection describing the sort order.</param>
        /// <param name="filters">A collection describing the filtering criteria.</param>
        /// <param name="Joins">A collection describing the dataset joins.</param>
        /// <returns>Returns a YetaWF.Core.DataProvider.DataProviderGetRecords object describing the data returned.</returns>
        public new async Task<DataProviderGetRecords<OBJTYPE>> GetRecordsAsync(int skip, int take, List<DataProviderSortInfo> sorts, List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            SQLBuilder sb = new SQLBuilder();
            await EnsureOpenAsync();
            if (Dataset == BaseDataset) {
                // we're reading the base table
                return await base.GetRecordsAsync(skip, take, sorts, filters, Joins: Joins);
            } else {
                // an explicit type is requested
                SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

                DataProviderGetRecords<OBJTYPE> recs = new DataProviderGetRecords<OBJTYPE>();

                string fullBaseTableName = sb.GetTable(Database, Dbo, BaseDataset);
                string fullTableName = sb.GetTable(Database, Dbo, Dataset);

                // get total # of records (only if a subset is requested)
                string selectCount = null;
                if (skip != 0 || take != 0) {
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

        /// <summary>
        /// Removes records using filtering criteria.
        /// </summary>
        /// <param name="filters">A collection describing the filtering criteria.</param>
        /// <returns>Returns the number of records removed.</returns>
        /// <remarks>
        /// This is not implemented as it is not required for module storage.
        /// </remarks>
        public new Task<int> RemoveRecordsAsync(List<DataProviderFilterInfo> filters) {
            throw new NotImplementedException();
        }

        internal List<PropertyData> GetBasePropertyData() {
            if (_basePropertyData == null)
                _basePropertyData = ObjectSupport.GetPropertyData(typeof(ModuleDefinition));
            return _basePropertyData;
        }
        private static List<PropertyData> _basePropertyData;

        internal new List<PropertyData> GetPropertyData() {
            if (_propertyData == null) {
                List<PropertyData> propData = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
                // subtract all properties that are already defined in the base type
                List<PropertyData> basePropData = GetBasePropertyData();
                _propertyData = new List<PropertyData>();
                foreach (PropertyData p in propData) {
                    if (p.HasAttribute(Data_PrimaryKey.AttributeName)) {
                        // The primary key has to be present in both derived and base table because they're used as foreign key
                        _propertyData.Add(p);
                    } else {
                        PropertyData first = (from bp in basePropData where bp.Name == p.Name select p).FirstOrDefault();
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

        /// <summary>
        /// Returns whether the data provider is installed and available.
        /// </summary>
        /// <returns>true if the data provider is installed and available, false otherwise.</returns>
        public new async Task<bool> IsInstalledAsync() {
            SQLManager sqlManager = new SQLManager();
            await EnsureOpenAsync();
            if (!sqlManager.HasTable(Conn, Database, Dbo, BaseDataset))
                return false;
            if (Dataset != BaseDataset) {
                if (!sqlManager.HasTable(Conn, Database, Dbo, Dataset))
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Installs all data models (files, tables, etc.) for the data provider.
        /// </summary>
        /// <param name="errorList">A collection of error strings in user displayable format.</param>
        /// <returns>true if the models were created successfully, false otherwise.
        /// If the models could not be created, <paramref name="errorList"/> contains the reason for the failure.</returns>
        /// <remarks>
        /// While a package is installed, all data models are installed by calling the InstallModelAsync method.</remarks>
        public new async Task<bool> InstallModelAsync(List<string> errorList) {
            await EnsureOpenAsync();
            if (Dataset == BaseDataset) throw new InternalError("Base dataset is not supported");
            bool success = CreateTableWithBaseType(Conn, Database, errorList);
            SQLGenericManagerCache.ClearCache();
            return success;
        }
        private bool CreateTableWithBaseType(SqlConnection conn, string dbName, List<string> errorList) {
            Type baseType = typeof(ModuleDefinition);
            List<string> columns = new List<string>();
            SQLGen sqlCreate = new SQLGen(conn, Languages, IdentitySeed, Logging);
            if (!sqlCreate.CreateTableFromModel(dbName, Dbo, BaseDataset, Key1Name, null, IdentityName, GetBasePropertyData(), baseType, errorList, columns,
                    TopMost: true,
                    SiteSpecific: SiteIdentity > 0,
                    DerivedDataTableName: "DerivedDataTableName", DerivedDataTypeName: "DerivedDataType", DerivedAssemblyName: "DerivedAssemblyName"))
                return false;
            return sqlCreate.CreateTableFromModel(dbName, Dbo, Dataset, Key1Name, null, SQLBase.IdentityColumn, GetPropertyData(), typeof(OBJTYPE), errorList, columns,
                TopMost: true,
                SiteSpecific: SiteIdentity > 0,
                ForeignKeyTable: BaseDataset);
        }

        /// <summary>
        /// Uninstalls all data models (files, tables, etc.) for the data provider.
        /// </summary>
        /// <param name="errorList">A collection of error strings in user displayable format.</param>
        /// <returns>true if the models were removed successfully, false otherwise.
        /// If the models could not be removed, <paramref name="errorList"/> contains the reason for the failure.</returns>
        /// <remarks>
        /// While a package is uninstalled, all data models are uninstalled by calling the UninstallModelAsync method.</remarks>
        public new async Task<bool> UninstallModelAsync(List<string> errorList) {
            await EnsureOpenAsync();
            if (Dataset == BaseDataset) throw new InternalError("Base dataset is not supported");
            try {
                await DropTableWithBaseType(Database, errorList);
                return true;
            } catch (Exception exc) {
                errorList.Add(string.Format("{0}: {1}", typeof(OBJTYPE).FullName, ErrorHandling.FormatExceptionMessage(exc)));
                return false;
            } finally {
                SQLGenericManagerCache.ClearCache();
            }
        }
        private async Task<bool> DropTableWithBaseType(string dbName, List<string> errorList) {
            SQLManager sqlManager = new SQLManager();
            try {
                if (sqlManager.HasTable(Conn, dbName, Dbo, Dataset)) {
                    // Remove all records from the table (this removes the records in BaseTableName also)
                    SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                    SQLBuilder sb = new SQLBuilder();
                    sb.Add($@"
DELETE {BaseDataset} FROM {BaseDataset}
    INNER JOIN {Dataset} ON {BaseDataset}.[{Key1Name}] = {Dataset}.[{Key1Name}]
                    ");
                    await sqlHelper.ExecuteNonQueryAsync(sb.ToString());
                    // then drop the table
                    SQLManager.DropTable(Conn, dbName, Dbo, Dataset);
                }
                if (sqlManager.HasTable(Conn, dbName, Dbo, BaseDataset)) {
                    SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                    SQLBuilder sb = new SQLBuilder();
                    sb.Add($@"
SELECT COUNT(*) FROM  {BaseDataset}
");
                    object val = await sqlHelper.ExecuteScalarAsync(sb.ToString());
                    int count = Convert.ToInt32(val);
                    if (count == 0)
                        SQLManager.DropTable(Conn, dbName, Dbo, Dataset);
                }
            } catch (Exception exc) {
                if (Logging) YetaWF.Core.Log.Logging.AddErrorLog("Couldn't drop table", exc);
                errorList.Add(string.Format("Couldn't drop table - {0}.", ErrorHandling.FormatExceptionMessage(exc)));
                return false;
            }
            return true;
        }
        /// <summary>
        /// Removes data when a site is deleted.
        /// </summary>
        /// <remarks>
        /// When a site is deleted, the RemoveSiteDataAsync method is called for all data providers.
        /// Data providers can then remove site-specific data as the site is removed.</remarks>
        public new async Task RemoveSiteDataAsync() { // remove site-specific data
            await EnsureOpenAsync();
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

        /// <summary>
        /// Imports data into the data provider.
        /// </summary>
        /// <param name="chunk">The zero-based chunk number as data is imported. The first call when importing begins specifies 0 as chunk number.</param>
        /// <param name="fileList">A collection of files to be imported. Files are automatically imported, so the data provider doesn't have to process this collection.</param>
        /// <param name="obj">The data to be imported.</param>
        /// <remarks>
        /// The ImportChunkAsync method is called to import data for site restores, page and module imports.
        ///
        /// When a data provider is called to import data, it is called repeatedly until no more data is available.
        /// Each time it is called, it is expected to import the chunk of data defined by <paramref name="obj"/>.
        /// Each time ImportChunkAsync method is called, the zero-based chunk number <paramref name="chunk"/> is incremented.
        ///
        /// The <paramref name="obj"/> parameter is provided without type but should be cast to
        /// YetaWF.Core.Serializers.SerializableList&lt;OBJTYPE&gt; as it is a collection of records to import. All records in the collection must be imported.
        /// </remarks>
        public new async Task ImportChunkAsync(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            await EnsureOpenAsync();
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

        /// <summary>
        /// Exports data from the data provider.
        /// </summary>
        /// <param name="chunk">The zero-based chunk number as data is exported. The first call when exporting begins specifies 0 as chunk number.</param>
        /// <param name="fileList">A collection of files. The data provider can add files to be exported to this collection when ExportChunkAsync is called.</param>
        /// <returns>Returns a YetaWF.Core.DataProvider.DataProviderExportChunk object describing the data exported.</returns>
        /// <remarks>
        /// The ExportChunkAsync method is called to export data for site backups, page and module exports.
        ///
        /// When a data provider is called to export data, it is called repeatedly until YetaWF.Core.DataProvider.DataProviderExportChunk.More is returned as false.
        /// Each time it is called, it is expected to export a chunk of data. The amount of data, i.e., the chunk size, is determined by the data provider.
        ///
        /// Each time ExportChunkAsync method is called, the zero-based chunk number <paramref name="chunk"/> is incremented.
        /// The data provider returns data in an instance of the YetaWF.Core.DataProvider.DataProviderExportChunk object.
        ///
        /// Files to be exported can be added to the <paramref name="fileList"/> collection.
        /// Only data records need to be added to the returned YetaWF.Core.DataProvider.DataProviderExportChunk object.
        /// </remarks>
        public new async Task<DataProviderExportChunk> ExportChunkAsync(int chunk, SerializableList<SerializableFile> fileList) {
            await EnsureOpenAsync();
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
        /// <summary>
        /// Called to translate the data managed by the data provider to another language.
        /// </summary>
        /// <param name="language">The target language (see LanguageSettings.json).</param>
        /// <param name="isHtml">A method that can be called by the data provider to test whether a string contains HTML.</param>
        /// <param name="translateStringsAsync">A method that can be called to translate a collection of simple strings into the new language. A simple string does not contain HTML or newline characters.</param>
        /// <param name="translateComplexStringAsync">A method that can be called to translate a collection of complex strings into the new language. A complex string can contain HTML and newline characters.</param>
        /// <remarks>
        /// The data provider has to retrieve all records and translate them as needed using the
        /// provided <paramref name="translateStringsAsync"/> and <paramref name="translateComplexStringAsync"/> methods, and save the translated data.
        ///
        /// The YetaWF.Core.Models.ObjectSupport.TranslateObject method can be used to translate all YetaWF.Core.Models.MultiString instances.
        ///
        /// The translated data should be stored separately from the default language (except MultiString, which is part of the record).
        /// Using the <paramref name="language"/> parameter, a different folder should be used to store the translated data.
        /// </remarks>
        public new async Task LocalizeModelAsync(string language, Func<string, bool> isHtml, Func<List<string>, Task<List<string>>> translateStringsAsync, Func<string, Task<string>> translateComplexStringAsync) {
            await EnsureOpenAsync();
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
