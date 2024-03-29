﻿/* Copyright © 2023 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Microsoft.Data.SqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;
using YetaWF.DataProvider.SQLGeneric;

namespace YetaWF.DataProvider.SQL {

    /// <summary>
    /// This class implements access to objects (records), with one primary key and without identity column.
    /// </summary>
    public partial class SQLSimpleObject<KEYTYPE, OBJTYPE> : SQLSimpleObjectBase<KEYTYPE, object, OBJTYPE> where KEYTYPE : notnull where OBJTYPE : notnull {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="options">A dictionary of options and optional parameters as provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.</param>
        /// <remarks>
        /// Data providers are instantiated when the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method is called, usually by an application data provider.
        /// </remarks>
        public SQLSimpleObject(Dictionary<string, object> options) : base(options) { }

    }

    /// <summary>
    /// This base class implements access to objects (records), with a primary and secondary key (composite) and without identity column.
    /// This base class is not intended for use by application data providers. These use one of the more specialized derived classes instead.
    /// </summary>
    public class SQLSimpleObjectBase<KEYTYPE, KEYTYPE2, OBJTYPE> : SQLBase, IDataProvider<KEYTYPE, OBJTYPE>, ISQLTableInfo where KEYTYPE : notnull where OBJTYPE : notnull {

        internal SQLSimpleObjectBase(Dictionary<string, object> options, bool HasKey2 = false) : base(options, HasKey2) { }

        /// <summary>
        /// Defines the chunk size used by SQL data providers when exporting/importing data using the methods
        /// YetaWF.Core.IO.IInstallableModel.ExportChunkAsync and YetaWF.Core.IO.IInstallableModel.ExportChunkAsync.ImportChunkAsync.
        /// </summary>
        internal const int ChunkSize = 100;

        internal bool Warnings = true;

        /// <summary>
        /// Retrieves the property information for the model used.
        /// </summary>
        /// <returns>List of property information.</returns>
        protected override List<PropertyData> GetPropertyData() {
            if (_propertyData == null)
                _propertyData = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
            return _propertyData;
        }
        List<PropertyData>? _propertyData;

        /// <summary>
        /// Retrieves one record from the database table that satisfies the specified primary key <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The primary key value.</param>
        /// <returns>Returns the record that satisfies the specified primary key. If no record exists null is returned.</returns>
        public Task<OBJTYPE?> GetAsync(KEYTYPE key) {
            return GetAsync(key, default(KEYTYPE2));
        }
        /// <summary>
        /// Retrieves one record from the database table that satisfies the specified keys.
        /// </summary>
        /// <param name="key">The primary key value.</param>
        /// <param name="key2">The secondary key value.</param>
        /// <returns>Returns the record that satisfies the specified primary and secondary keys. If no record exists null is returned.</returns>
        public async Task<OBJTYPE?> GetAsync(KEYTYPE key, KEYTYPE2? key2) {
            SQLBuilder sb = new SQLBuilder();
            await EnsureOpenAsync();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string? joins = null;// RFFU
            string fullTableName = sb.GetTable(Database, Dbo, Dataset);
            string? calcProps = await CalculatedPropertiesAsync(typeof(OBJTYPE));
            string? andKey2 = HasKey2 ? "AND " + sqlHelper.Expr(Key2Name, "=", key2) : null;

            List<PropertyData> propData = GetPropertyData();
            string subTablesSelects = SubTablesSelectsUsingJoin(sqlHelper, Dataset, key, key2, propData, typeof(OBJTYPE));

            string script = $@"
SELECT TOP 1 *
    {calcProps}
FROM {fullTableName} WITH(NOLOCK) {joins}
WHERE {sqlHelper.Expr(Key1Name, "=", key)} {andKey2} {AndSiteIdentity}  --- result set
;

{subTablesSelects}

{sqlHelper.DebugInfo}";

            using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(script)) {
                if (! (YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) return default(OBJTYPE);
                OBJTYPE obj = sqlHelper.CreateObject<OBJTYPE>(reader);
                if (!string.IsNullOrWhiteSpace(subTablesSelects)) {
                    await ReadSubTablesAsync(sqlHelper, reader, Dataset, obj, propData, typeof(OBJTYPE));
                }
                return obj;
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
        public async Task<bool> AddAsync(OBJTYPE obj) {
            SQLBuilder sb = new SQLBuilder();
            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = sb.GetTable(Database, Dbo, Dataset);
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
                    object? val = await sqlHelper.ExecuteScalarAsync(script);
                    identity = Convert.ToInt32(val);
                } else {
                    await sqlHelper.ExecuteNonQueryAsync(script);
                }
            } catch (Exception exc) {
                SqlException? sqlExc = exc as SqlException;
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
        public async Task<UpdateStatusEnum> UpdateAsync(KEYTYPE origKey, KEYTYPE newKey, OBJTYPE obj) {
            return await UpdateAsync(origKey, default(KEYTYPE2), newKey, default(KEYTYPE2), obj);
        }

        /// <summary>
        /// Updates an existing record with the specified existing primary and secondary keys <paramref name="origKey"/> in the database table.
        /// The primary and secondary keys can be changed to the new values in <paramref name="newKey"/> and <paramref name="newKey2"/>.
        /// </summary>
        /// <param name="origKey">The original primary key value of the record.</param>
        /// <param name="origKey2">The original secondary key value of the record.</param>
        /// <param name="newKey">The new primary key value of the record. This may be the same value as <paramref name="origKey"/>. </param>
        /// <param name="newKey2">The new secondary key value of the record. This may be the same value as <paramref name="origKey2"/>. </param>
        /// <param name="obj">The object being updated.</param>
        /// <returns>Returns a status indicator.</returns>
        public async Task<UpdateStatusEnum> UpdateAsync(KEYTYPE origKey, KEYTYPE2? origKey2, KEYTYPE newKey, KEYTYPE2? newKey2, OBJTYPE obj) {
            SQLBuilder sb = new SQLBuilder();
            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = sb.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = GetPropertyData();
            string setColumns = SetColumns(sqlHelper, Dataset, propData, obj, typeof(OBJTYPE));
            string? andKey2 = HasKey2 ? "AND " + sqlHelper.Expr(Key2Name, "=", origKey2) : null;

            string? subTablesUpdates = SubTablesUpdates(sqlHelper, Dataset, obj, propData, typeof(OBJTYPE));

            string? warningsOff = !Warnings ? " SET ANSI_WARNINGS OFF" : null;
            string? warningsOn = !Warnings ? " SET ANSI_WARNINGS ON" : null;

            string scriptMain = $@"
{warningsOff}

UPDATE {fullTableName}
SET {setColumns}
WHERE {sqlHelper.Expr(Key1Name, "=", origKey)} {andKey2} {AndSiteIdentity}
;
SELECT @@ROWCOUNT --- result set

{warningsOn}

{sqlHelper.DebugInfo}";

            string scriptWithSub = $@"
{warningsOff}

DECLARE @__IDENTITY int;
SELECT @__IDENTITY = [{IdentityNameOrDefault}] FROM {fullTableName}
WHERE {sqlHelper.Expr(Key1Name, "=", origKey)} {andKey2} {AndSiteIdentity}

UPDATE {fullTableName}
SET {setColumns}
WHERE [{IdentityNameOrDefault}] = @__IDENTITY
;
SELECT @@ROWCOUNT --- result set

{subTablesUpdates}

{warningsOn}
{sqlHelper.DebugInfo}";

            string script = (string.IsNullOrWhiteSpace(subTablesUpdates)) ? scriptMain : scriptWithSub;

            try {
                object? val = await sqlHelper.ExecuteScalarAsync(script);
                int changed = Convert.ToInt32(val);
                if (changed == 0)
                    return UpdateStatusEnum.RecordDeleted;
                if (changed > 1)
                    throw new InternalError($"Update failed - {changed} records updated");
            } catch (Exception exc) {
                if (!newKey.Equals(origKey)) {
                    SqlException? sqlExc = exc as SqlException;
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
        public async Task<bool> RemoveAsync(KEYTYPE key) {
            return await RemoveAsync(key, default(KEYTYPE2));
        }
        /// <summary>
        /// Removes an existing record with the specified primary and secondary keys.
        /// </summary>
        /// <param name="key">The primary key value of the record to remove.</param>
        /// <param name="key2">The secondary key value of the record to remove.</param>
        /// <returns>Returns true if the record was removed, or false if the record was not found. Other errors cause an exception.</returns>
        public async Task<bool> RemoveAsync(KEYTYPE key, KEYTYPE2? key2) {
            SQLBuilder sb = new SQLBuilder();
            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = sb.GetTable(Database, Dbo, Dataset);
            string? andKey2 = HasKey2 ? "AND " + sqlHelper.Expr(Key2Name, "=", key2) : null;

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

            object? val = await sqlHelper.ExecuteScalarAsync(script);
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
        /// </remarks>
        public async Task<OBJTYPE?> GetOneRecordAsync(List<DataProviderFilterInfo>? filters, List<JoinData>? Joins = null) {
            await EnsureOpenAsync();
            filters = NormalizeFilter(typeof(OBJTYPE), filters);
            DataProviderGetRecords<OBJTYPE> recs = await GetMainTableRecordsAsync(0, 1, null, filters, Joins: Joins);
            return recs.Data.FirstOrDefault();
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
        public async Task<DataProviderGetRecords<OBJTYPE>> GetRecordsAsync(int skip, int take, List<DataProviderSortInfo>? sorts, List<DataProviderFilterInfo>? filters, List<JoinData>? Joins = null) {
            await EnsureOpenAsync();
            filters = NormalizeFilter(typeof(OBJTYPE), filters);
            sorts = NormalizeSort(typeof(OBJTYPE), sorts);
            return await GetMainTableRecordsAsync(skip, take, sorts, filters, Joins: Joins);
        }

        /// <summary>
        /// Removes records using filtering criteria.
        /// </summary>
        /// <param name="filters">A collection describing the filtering criteria.</param>
        /// <returns>Returns the number of records removed.</returns>
        public async Task<int> RemoveRecordsAsync(List<DataProviderFilterInfo>? filters) {
            SQLBuilder sb = new SQLBuilder();
            await EnsureOpenAsync();
            filters = NormalizeFilter(typeof(OBJTYPE), filters);

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = sb.GetTable(Database, Dbo, Dataset);
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
FROM {fullTableName}
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

            object? val = await sqlHelper.ExecuteScalarAsync(script);
            int deleted = Convert.ToInt32(val);
            return deleted;
        }

        internal async Task<DataProviderGetRecords<OBJTYPE>> GetMainTableRecordsAsync(int skip, int take, List<DataProviderSortInfo>? sorts, List<DataProviderFilterInfo>? filters, List<JoinData>? Joins = null) {
            SQLBuilder sb = new SQLBuilder();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            DataProviderGetRecords<OBJTYPE> recs = new DataProviderGetRecords<OBJTYPE>();

            string fullTableName = sb.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = GetPropertyData();
            Dictionary<string, string> visibleColumns = await GetVisibleColumnsAsync(Database, Dbo, Dataset, typeof(OBJTYPE), Joins);
            string columnList = MakeColumnList(sqlHelper, visibleColumns, Joins);
            string joins = await MakeJoinsAsync(sqlHelper, Joins);
            string filter = MakeFilter(sqlHelper, filters, visibleColumns);
            string? calcProps = await CalculatedPropertiesAsync(typeof(OBJTYPE));
            // get total # of records (only if a subset is requested)
            string? selectCount = null;
            if (skip != 0 || take != 0) {
                sb = new SQLBuilder();
                sb.Add($"SELECT COUNT(*) FROM {fullTableName} WITH(NOLOCK) {joins} {filter} ");
                selectCount = sb.ToString();
            }

            string orderBy;
            {
                sb = new SQLBuilder();
                if (sorts == null || sorts.Count == 0)
                    sorts = new List<DataProviderSortInfo> { new DataProviderSortInfo { Field = Key1Name, Order = DataProviderSortInfo.SortDirection.Ascending } };
                sb.AddOrderBy(visibleColumns, sorts, skip, take);
                orderBy = sb.ToString();
            }

            string script = $@"
{selectCount} --- result set
SELECT {columnList} --- result set
    {calcProps}
FROM {fullTableName} WITH(NOLOCK)
{joins}
{filter}
{orderBy}

{sqlHelper.DebugInfo}";

            SQLHelper subSqlHelper = new SQLHelper(Conn, null, Languages);
            sb = new SQLBuilder();

            using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(script)) {
                if (skip != 0 || take != 0) {
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) throw new InternalError("Expected # of records");
                    recs.Total = reader.GetInt32(0);
                    if (!(YetaWFManager.IsSync() ? reader.NextResult() : await reader.NextResultAsync())) throw new InternalError("Expected next result set (main table)");
                }
                while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) {
                    OBJTYPE o = sqlHelper.CreateObject<OBJTYPE>(reader);
                    recs.Data.Add(o);

                    PropertyInfo piIdent = ObjectSupport.GetProperty(typeof(OBJTYPE), Key1Name);
                    KEYTYPE keyVal = (KEYTYPE)piIdent.GetValue(o)!;
                    KEYTYPE2? key2Val = default(KEYTYPE2);
                    if (HasKey2) {
                        PropertyInfo piIdent2 = ObjectSupport.GetProperty(typeof(OBJTYPE), Key2Name);
                        key2Val = (KEYTYPE2?)piIdent2.GetValue(o);
                    }
                    sb.Append( SubTablesSelectsUsingJoin(subSqlHelper, Dataset, keyVal, key2Val, propData, typeof(OBJTYPE)) );
                }
            }
            string subTablesSelects = sb.ToString();
            if (!string.IsNullOrWhiteSpace(subTablesSelects)) {
                subTablesSelects += $@"

{subSqlHelper.DebugInfo}";
                using (SqlDataReader reader = await subSqlHelper.ExecuteReaderAsync(subTablesSelects)) {
                    await ReadSubTablesMatchupAsync(subSqlHelper, reader, Dataset, recs.Data, propData, typeof(OBJTYPE));
                }
            }
            if (skip == 0 && take == 0)
                recs.Total = recs.Data.Count;
            return recs;
        }

        internal string SubTablesSelects(string tableName, List<PropertyData> propData, Type tpContainer) {
            SQLBuilder sb = new SQLBuilder();
            List<SQLGenericGen.SubTableInfo> subTables = SQLGen.GetSubTables(tableName, propData);
            if (subTables.Count > 0) {
                foreach (SQLGenericGen.SubTableInfo subTable in subTables) {
                    sb.Add($@"
    SELECT * FROM {sb.BuildFullTableName(Database, Dbo, subTable.Name)} WHERE {sb.BuildFullColumnName(subTable.Name, SubTableKeyColumn)} = @ident ; --- result set
");
                }
            }
            return sb.ToString();
        }

        internal string SubTablesSelectsUsingJoin(SQLHelper sqlHelper, string tableName, KEYTYPE key, KEYTYPE2? key2, List<PropertyData> propData, Type tpContainer) {
            SQLBuilder sb = new SQLBuilder();
            List<SQLGenericGen.SubTableInfo> subTables = SQLGen.GetSubTables(tableName, propData);
            if (subTables.Count > 0) {
                string keyExpr = (key == null || key.Equals(default(KEYTYPE)) ? "1=1" : $"{sb.BuildFullColumnName(Database, Dbo, tableName, Key1Name)} = {sqlHelper.AddTempParam(key)}");
                string? andKey2 = HasKey2 ? "AND " + sqlHelper.Expr(Key2Name, "=", key2) : null;

                foreach (SQLGenericGen.SubTableInfo subTable in subTables) {
                    sb.Add($@"
    SELECT * FROM {sb.BuildFullTableName(Database, Dbo, subTable.Name)}   --- result set
    INNER JOIN {sb.BuildFullTableName(Database, Dbo, tableName)} ON {sb.BuildFullColumnName(tableName, IdentityNameOrDefault)} = {sb.BuildFullColumnName(subTable.Name, SubTableKeyColumn)}
    WHERE {keyExpr} {andKey2} {AndSiteIdentity}
");
                    sb.Add(@"
;
                        ");
                }
            }
            return sb.ToString();
        }

        internal async Task ReadSubTablesAsync(SQLHelper sqlHelper, SqlDataReader reader, string tableName, OBJTYPE container, List<PropertyData> propData, Type tpContainer) {

            List<SQLGenericGen.SubTableInfo> subTables = SQLGen.GetSubTables(tableName, propData);
            foreach (SQLGenericGen.SubTableInfo subTable in subTables) {
                object? subContainer = subTable.PropInfo.GetValue(container);
                if (subContainer == null) throw new InternalError($"{nameof(ReadSubTablesAsync)} encountered a enumeration property that is null");

                // find the Add method for the collection so we can add each item as its read
                MethodInfo? addMethod = subTable.PropInfo.PropertyType.GetMethod("Add", new Type[] { subTable.Type });
                if (addMethod == null) throw new InternalError($"{nameof(ReadSubTablesAsync)} encountered a enumeration property that doesn't have an Add method");

                if (!(YetaWFManager.IsSync() ? reader.NextResult() : await reader.NextResultAsync())) throw new InternalError("Expected next result set (subtable)");
                while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) {
                    object obj = sqlHelper.CreateObject(reader, subTable.Type);
                    addMethod.Invoke(subContainer, new object[] { obj });
                }
            }
        }

        internal async Task ReadSubTablesMatchupAsync(SQLHelper sqlHelper, SqlDataReader reader, string tableName, List<OBJTYPE> containers, List<PropertyData> propData, Type tpContainer) {

            // extract identities from container list so we can match sub-objects more easily
            List<int> identities = GetIdentities(containers);

            List<SQLGenericGen.SubTableInfo> subTables = SQLGen.GetSubTables(tableName, propData);

            for ( ; ; ) {
                foreach (SQLGenericGen.SubTableInfo subTable in subTables) {

                    while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) {

                        // find the Add method for the collection so we can add each item as its read
                        MethodInfo? addMethod = subTable.PropInfo.PropertyType.GetMethod("Add", new Type[] { subTable.Type });
                        if (addMethod == null) throw new InternalError($"{nameof(ReadSubTablesMatchupAsync)} encountered a enumeration property that doesn't have an Add method");

                        int key = (int)reader[SubTableKeyColumn];
                        object obj = sqlHelper.CreateObject(reader, subTable.Type);
                        // find the container this subtable entry matches
                        AddToContainer(containers, identities, subTable, obj, key, addMethod);
                    }
                    if (!(YetaWFManager.IsSync() ? reader.NextResult() : await reader.NextResultAsync()))
                        return;
                }
            }
        }

        private List<int> GetIdentities(List<OBJTYPE> containers) {
            List<int> list = new List<int>();
            PropertyInfo piIdent = ObjectSupport.GetProperty(typeof(OBJTYPE), IdentityNameOrDefault);
            if (piIdent.PropertyType != typeof(int)) throw new InternalError($"Object identities must be of type int in {typeof(OBJTYPE).FullName}");
            foreach (OBJTYPE c in containers) {
                int identity = (int)piIdent.GetValue(c)!;
                list.Add(identity);
            }
            return list;
        }

        private void AddToContainer(List<OBJTYPE> containers, List<int> identities, SQLGenericGen.SubTableInfo subTable, object obj, int key, MethodInfo addMethod) {

            int index = identities.IndexOf(key); // find the index of the matching identity/container
            if (index < 0) throw new InternalError($"Subtable {subTable.Name} has key {key} that doesn't match any main record");

            OBJTYPE container = containers[index];
            object? subContainer = subTable.PropInfo.GetValue(container);
            if (subContainer == null) throw new InternalError($"{nameof(AddToContainer)} encountered a enumeration property that is null");

            addMethod.Invoke(subContainer, new object[] { obj });
        }

        internal string SubTablesInserts(SQLHelper sqlHelper, string tableName, object container, List<PropertyData> propData, Type tpContainer) {
            SQLBuilder sb = new SQLBuilder();
            List<SQLGenericGen.SubTableInfo> subTables = SQLGen.GetSubTables(tableName, propData);
            foreach (SQLGenericGen.SubTableInfo subTable in subTables) {
                List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                IEnumerable ienum = (IEnumerable)subTable.PropInfo.GetValue(container)!;
                foreach (object obj in ienum) {
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
        internal string? SubTablesUpdates(SQLHelper sqlHelper, string tableName, object container, List<PropertyData> propData, Type tpContainer) {
            SQLBuilder sb = new SQLBuilder();
            List<SQLGenericGen.SubTableInfo> subTables = SQLGen.GetSubTables(tableName, propData);
            if (subTables.Count == 0) return null;
            sb.Add("BEGIN TRANSACTION Upd;");
            foreach (SQLGenericGen.SubTableInfo subTable in subTables) {
                sb.Add($@"
    DELETE FROM {subTable.Name} WITH(SERIALIZABLE) WHERE {SQLBase.SubTableKeyColumn} = @__IDENTITY ;
");
                List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                IEnumerable ienum = (IEnumerable)subTable.PropInfo.GetValue(container)!;
                foreach (object obj in ienum) {
                    string columns = GetColumnList(subPropData, subTable.Type, "", false, SubTable: true);
                    string values = GetValueList(sqlHelper, Dataset, obj, subPropData, subTable.Type, "", false, SubTable: true);
                    sb.Add($@"
    INSERT INTO {subTable.Name}
        ({columns})
        VALUES ({values}) ;
");
                }
            }
            sb.Add("COMMIT TRANSACTION Upd;");
            return sb.ToString();
        }
        internal string SubTablesDeletes(string tableName, List<PropertyData> propData, Type tpContainer) {
            SQLBuilder sb = new SQLBuilder();
            List<SQLGenericGen.SubTableInfo> subTables = SQLGen.GetSubTables(tableName, propData);
            foreach (SQLGenericGen.SubTableInfo subTable in subTables) {
                sb.Add($@"
    DELETE FROM {sb.BuildFullTableName(Database, Dbo, subTable.Name)} WHERE {sb.BuildFullColumnName(subTable.Name, SubTableKeyColumn)} = @ident ;
");
            }
            return sb.ToString();
        }

        // IINSTALLMODEL
        // IINSTALLMODEL
        // IINSTALLMODEL

        /// <summary>
        /// Returns whether the data provider is installed and available.
        /// </summary>
        /// <returns>true if the data provider is installed and available, false otherwise.</returns>
        public async Task<bool> IsInstalledAsync() {
            SQLManager sqlManager = new SQLManager();
            await EnsureOpenAsync();
            return sqlManager.HasTable(Conn, Database, Dbo, Dataset);
        }

        /// <summary>
        /// Installs all data models (files, tables, etc.) for the data provider.
        /// </summary>
        /// <param name="errorList">A collection of error strings in user displayable format.</param>
        /// <returns>true if the models were created successfully, false otherwise.
        /// If the models could not be created, <paramref name="errorList"/> contains the reason for the failure.</returns>
        /// <remarks>
        /// While a package is installed, all data models are installed by calling the InstallModelAsync method.</remarks>
        public async Task<bool> InstallModelAsync(List<string> errorList) {
            await EnsureOpenAsync();
            SQLGen sqlCreate = new SQLGen(Conn, Languages, IdentitySeed, Logging);
            //TODO: could asyncify but probably not worth it as this is used during install/startup only
            bool success = sqlCreate.CreateTableFromModel(Database, Dbo, Dataset, Key1Name, HasKey2 ? Key2Name : null, IdentityName, GetPropertyData(), typeof(OBJTYPE), errorList,
                SiteSpecific: SiteIdentity > 0,
                TopMost: true);
            SQLGenericManagerCache.ClearCache();
            return success;
        }

        /// <summary>
        /// Uninstalls all data models (files, tables, etc.) for the data provider.
        /// </summary>
        /// <param name="errorList">A collection of error strings in user displayable format.</param>
        /// <returns>true if the models were removed successfully, false otherwise.
        /// If the models could not be removed, <paramref name="errorList"/> contains the reason for the failure.</returns>
        /// <remarks>
        /// While a package is uninstalled, all data models are uninstalled by calling the UninstallModelAsync method.</remarks>
        public async Task<bool> UninstallModelAsync(List<string> errorList) {
            await EnsureOpenAsync();
            try {
                SQLBuilder sb = new SQLBuilder();
                SQLGen sqlCreate = new SQLGen(Conn, Languages, IdentitySeed, Logging);
                List<PropertyData> propData = GetPropertyData();
                List<SQLGenericGen.SubTableInfo> subTables = SQLGen.GetSubTables(Dataset, propData);
                foreach (SQLGenericGen.SubTableInfo subTable in subTables) {
                    //TODO: could asyncify but probably not worth it as this is used during install/startup only
                    sqlCreate.DropTable(Database, Dbo, subTable.Name, errorList);
                }
                sqlCreate.DropTable(Database, Dbo, Dataset, errorList);
                return true;
            } catch (Exception exc) {
                errorList.Add(string.Format("{0}: {1}", typeof(OBJTYPE).FullName, ErrorHandling.FormatExceptionMessage(exc)));
                return false;
            } finally {
                SQLGenericManagerCache.ClearCache();
            }
        }

        /// <summary>
        /// Adds data for a new site.
        /// </summary>
        /// <remarks>
        /// When a new site is created, the AddSiteDataAsync method is called for all data providers.
        /// Data providers can then add site-specific data as the new site is added.</remarks>
        public Task AddSiteDataAsync() { return Task.CompletedTask; }

        /// <summary>
        /// Removes data when a site is deleted.
        /// </summary>
        /// <remarks>
        /// When a site is deleted, the RemoveSiteDataAsync method is called for all data providers.
        /// Data providers can then remove site-specific data as the site is removed.</remarks>
        public async Task RemoveSiteDataAsync() { // remove site-specific data
            SQLBuilder sb = new SQLBuilder();
            await EnsureOpenAsync();
            if (SiteIdentity > 0) {
                string fullTableName = sb.GetTable(Database, Dbo, Dataset);
                SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                sb.Add($@"
DELETE FROM {fullTableName} WHERE [{SiteColumn}] = {SiteIdentity}
;
");
                // subtable data is removed by delete cascade
                await sqlHelper.ExecuteScalarAsync(sb.ToString());
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
        public async Task ImportChunkAsync(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            await EnsureOpenAsync();
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
        public async Task<DataProviderExportChunk> ExportChunkAsync(int chunk, SerializableList<SerializableFile> fileList) {
            await EnsureOpenAsync();

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
        public async Task LocalizeModelAsync(string language, Func<string, bool> isHtml, Func<List<string>, Task<List<string>>> translateStringsAsync, Func<string, Task<string>> translateComplexStringAsync) {
            await EnsureOpenAsync();
            await LocalizeModelAsync(language, isHtml, translateStringsAsync, translateComplexStringAsync,
                async (int offset, int skip) => {
                    return await GetRecordsAsync(offset, skip, null, null);
                },
                async (OBJTYPE record, PropertyInfo pi, PropertyInfo? pi2) => {
                    UpdateStatusEnum status;
                    KEYTYPE key1 = (KEYTYPE)pi.GetValue(record)!;
                    Warnings = false; // we're turning warnings off in case strings get truncated
                    try {
                        if (HasKey2) {
                            KEYTYPE2? key2 = (KEYTYPE2?)pi2!.GetValue(record);
                            status = await UpdateAsync(key1, key2, key1, key2, record);
                        } else {
                            status = await UpdateAsync(key1, key1, record);
                        }
                    } finally {
                        Warnings = true;// turn warnings back on
                    }
                    return status;
                });
        }
        /// <summary>
        /// Called to translate the data managed by the data provider to another language.
        /// </summary>
        /// <param name="language">The target language (see LanguageSettings.json).</param>
        /// <param name="isHtml">A method that can be called by the data provider to test whether a string contains HTML.</param>
        /// <param name="translateStringsAsync">A method that can be called to translate a collection of simple strings into the new language. A simple string does not contain HTML or newline characters.</param>
        /// <param name="translateComplexStringAsync">A method that can be called to translate a collection of complex strings into the new language. A complex string can contain HTML and newline characters.</param>
        /// <param name="getRecords"></param>
        /// <param name="saveRecordAsync"></param>
        /// <remarks>
        /// This is used by derived classes to translate the data managed by the data provider to another language.
        /// The derived class provides <paramref name="getRecords"/> and <paramref name="saveRecordAsync"/> methods, which are used to retrieve, translate and save the data.
        /// The YetaWF.Core.Models.ObjectSupport.TranslateObject method is to translate all YetaWF.Core.Models.MultiString instances.
        /// </remarks>
        protected async Task LocalizeModelAsync(string language,
                Func<string, bool> isHtml,
                Func<List<string>, Task<List<string>>> translateStringsAsync, Func<string, Task<string>> translateComplexStringAsync, Func<int, int, Task<DataProviderGetRecords<OBJTYPE>>> getRecords, Func<OBJTYPE, PropertyInfo, PropertyInfo?, Task<UpdateStatusEnum>> saveRecordAsync) {

            const int RECORDS = 20;

            List<PropertyInfo> props = ObjectSupport.GetProperties(typeof(OBJTYPE));
            PropertyInfo key1Prop = ObjectSupport.GetProperty(typeof(OBJTYPE), Key1Name);
            PropertyInfo? key2Prop = null;
            if (HasKey2)
                key2Prop = ObjectSupport.GetProperty(typeof(OBJTYPE), Key2Name);

            for (int offset = 0; ;) {
                DataProviderGetRecords<OBJTYPE> data = await getRecords(offset, RECORDS);
                if (data.Data.Count == 0)
                    break;
                foreach (OBJTYPE record in data.Data) {
                    bool changed = await ObjectSupport.TranslateObject(record, language, isHtml, translateStringsAsync, translateComplexStringAsync, props);
                    if (changed) {
                        UpdateStatusEnum status = await saveRecordAsync(record, key1Prop, key2Prop);
                        if (status != UpdateStatusEnum.OK)
                            throw new InternalError($"Update failed for type {typeof(OBJTYPE).FullName} ({status})");
                    }
                }
                offset += data.Data.Count;
                if (offset >= data.Total)
                    break;
            }
        }
    }
}
