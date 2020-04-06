/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
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
using System.Data;
using YetaWF.Core.Support.Serializers;
using YetaWF.Core.Language;
#if MVC6
using Microsoft.Data.SqlClient;
#else
using System.Data.SqlClient;
#endif

namespace YetaWF.DataProvider.SQL {

    /// <summary>
    /// This class implements access to objects (records), with one primary key and without identity column.
    /// </summary>
    public partial class SQLSimpleObject<KEYTYPE, OBJTYPE> : SQLSimpleObjectBase<KEYTYPE, object, OBJTYPE> {

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
    public class SQLSimpleObjectBase<KEYTYPE, KEYTYPE2, OBJTYPE> : SQLBase, IDataProvider<KEYTYPE, OBJTYPE>, ISQLTableInfo {

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
        List<PropertyData> _propertyData;

        /// <summary>
        /// Retrieves one record from the database table that satisfies the specified keys.
        /// </summary>
        /// <param name="key">The primary key value.</param>
        /// <returns>Returns the record that satisfies the specified keys. If no record exists null is returned.</returns>
        public Task<OBJTYPE> GetAsync(KEYTYPE key) {
            return GetAsync(key, default(KEYTYPE2));
        }
        /// <summary>
        /// Retrieves one record from the database table that satisfies the specified keys.
        /// </summary>
        /// <param name="key">The primary key value.</param>
        /// <param name="key2">The secondary key value.</param>
        /// <returns>Returns the record that satisfies the specified keys. If no record exists null is returned.</returns>
        public async Task<OBJTYPE> GetAsync(KEYTYPE key, KEYTYPE2 key2) {

            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            sqlHelper.AddParam("Key1Val", key);
            if (HasKey2)
                sqlHelper.AddParam("Key2Val", key2);
            if (SiteIdentity > 0)
                sqlHelper.AddParam(SQLGen.ValSiteIdentity, SiteIdentity);

            using (SqlDataReader reader = await sqlHelper.ExecuteReaderStoredProcAsync($@"[{Dbo}].[{Dataset}__Get]")) {
                if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) return default(OBJTYPE);
                OBJTYPE obj = sqlHelper.CreateObject<OBJTYPE>(reader);
                await ReadSubTablesAsync(sqlHelper, reader, Dataset, obj, typeof(OBJTYPE));
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

            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            GetParameterList(sqlHelper, obj, Database, Dbo, Dataset, GetPropertyData(), Prefix: null, TopMost: true, SiteSpecific: SiteIdentity > 0, WithDerivedInfo: false, SubTable: false);

            try {
                using (SqlDataReader reader = await sqlHelper.ExecuteReaderStoredProcAsync($@"[{Dbo}].[{Dataset}__Add]")) {
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) return false;
                    int identity = Convert.ToInt32(reader[0]);
                    if (identity <= 0)
                        throw new InternalError($"{nameof(AddAsync)} invalid identity {identity} returned");
                    if (HasIdentity(IdentityName)) {
                        PropertyInfo piIdent = ObjectSupport.GetProperty(typeof(OBJTYPE), IdentityName);
                        if (piIdent.PropertyType != typeof(int)) throw new InternalError($"Object identities must be of type int in {typeof(OBJTYPE).FullName}");
                        piIdent.SetValue(obj, identity);
                    }
                }
            } catch (Exception exc) {
                SqlException sqlExc = exc as SqlException;
                if (sqlExc != null && sqlExc.Number == 2627) // already exists
                    return false;
                throw new InternalError($"{nameof(AddAsync)} failed for type {typeof(OBJTYPE).FullName} - {ErrorHandling.FormatExceptionMessage(exc)}");
            }
            return true;
        }

        /// <summary>
        /// Updates an existing record with the specified existing primary keys in the database table.
        /// The primary keys can be changed to new values.
        /// </summary>
        /// <param name="origKey">The original primary key value of the record.</param>
        /// <param name="newKey">The new primary key value of the record. This may be the same value as <paramref name="origKey"/>. </param>
        /// <param name="obj">The object being updated.</param>
        /// <returns>Returns a status indicator.</returns>
        public async Task<UpdateStatusEnum> UpdateAsync(KEYTYPE origKey, KEYTYPE newKey, OBJTYPE obj) {
            return await UpdateAsync(origKey, default(KEYTYPE2), newKey, default(KEYTYPE2), obj);
        }

        /// <summary>
        /// Updates an existing record with the specified existing primary keys in the database table.
        /// The primary keys can be changed to new values.
        /// </summary>
        /// <param name="origKey">The original primary key value of the record.</param>
        /// <param name="origKey2">The original secondary key value of the record.</param>
        /// <param name="newKey">The new primary key value of the record. This may be the same value as <paramref name="origKey"/>. </param>
        /// <param name="newKey2">The new secondary key value of the record. This may be the same value as <paramref name="origKey2"/>. </param>
        /// <param name="obj">The object being updated.</param>
        /// <returns>Returns a status indicator.</returns>
        public async Task<UpdateStatusEnum> UpdateAsync(KEYTYPE origKey, KEYTYPE2 origKey2, KEYTYPE newKey, KEYTYPE2 newKey2, OBJTYPE obj) {

            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            GetParameterList(sqlHelper, obj, Database, Dbo, Dataset, GetPropertyData(), Prefix: null, TopMost: true, SiteSpecific: SiteIdentity > 0, WithDerivedInfo: false, SubTable: false);
            sqlHelper.AddParam("Key1Val", origKey);
            if (HasKey2)
                sqlHelper.AddParam("Key2Val", origKey2);

            try {
                using (SqlDataReader reader = await sqlHelper.ExecuteReaderStoredProcAsync($@"[{Dbo}].[{Dataset}__Update]")) {
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) return 0;
                    int changed = Convert.ToInt32(reader[0]);
                    if (changed == 0)
                        return UpdateStatusEnum.RecordDeleted;
                    if (changed != 1)
                        throw new InternalError($"Update failed - {changed} records updated");
                }
            } catch (Exception exc) {
                if (!newKey.Equals(origKey)) {
                    SqlException sqlExc = exc as SqlException;
                    if (sqlExc != null && sqlExc.Number == 2627) // already exists
                        return UpdateStatusEnum.NewKeyExists;
                }
                throw new InternalError($"{nameof(UpdateAsync)} failed for type {typeof(OBJTYPE).FullName} - {ErrorHandling.FormatExceptionMessage(exc)}");
            }
            return UpdateStatusEnum.OK;
        }

        /// <summary>
        /// Removes an existing record with the specified keys.
        /// </summary>
        /// <param name="key">The primary key value of the record to remove.</param>
        /// <returns>Returns true if the record was removed, or false if the record was not found. Other errors cause an exception.</returns>
        public async Task<bool> RemoveAsync(KEYTYPE key) {
            return await RemoveAsync(key, default(KEYTYPE2));
        }
        /// <summary>
        /// Removes an existing record with the specified keys.
        /// </summary>
        /// <param name="key">The primary key value of the record to remove.</param>
        /// <param name="key2">The secondary key value of the record to remove.</param>
        /// <returns>Returns true if the record was removed, or false if the record was not found. Other errors cause an exception.</returns>
        public async Task<bool> RemoveAsync(KEYTYPE key, KEYTYPE2 key2) {

            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            sqlHelper.AddParam("Key1Val", key);
            if (HasKey2)
                sqlHelper.AddParam("Key2Val", key2);
            if (SiteIdentity > 0)
                sqlHelper.AddParam(SQLGen.ValSiteIdentity, SiteIdentity);

            try {
                using (SqlDataReader reader = await sqlHelper.ExecuteReaderStoredProcAsync($@"[{Dbo}].[{Dataset}__Remove]")) {
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) return false;
                    int removed = Convert.ToInt32(reader[0]);
                    return removed > 0;
                }
            } catch (Exception exc) {
                SqlException sqlExc = exc as SqlException;
                if (sqlExc != null && sqlExc.Number == 547) // ref integrity
                    return false;
                throw new InternalError($"{nameof(RemoveAsync)} failed for type {typeof(OBJTYPE).FullName} - {ErrorHandling.FormatExceptionMessage(exc)}");
            }
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
        public async Task<OBJTYPE> GetOneRecordAsync(List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            DataProviderGetRecords<OBJTYPE> recs = await GetRecordsAsync(0, 1, null, filters, Joins: Joins);
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
        public async Task<DataProviderGetRecords<OBJTYPE>> GetRecordsAsync(int skip, int take, List<DataProviderSortInfo> sorts, List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            if (Joins == null)
                Joins = new List<JoinData>();

            await EnsureOpenAsync();

            SQLManager sqlManager = new SQLManager();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            SQLGen sqlCreate = new SQLGen(Conn, Languages, IdentitySeed, Logging);

            filters = NormalizeFilter(typeof(OBJTYPE), filters);
            sorts = NormalizeSort(typeof(OBJTYPE), sorts);

            SQLBuilder sb = new SQLBuilder();
            string fullTableName = sb.GetTable(Database, Dbo, Dataset);

            Dictionary<string, string> visibleColumns = Joins.Count > 0 ? new Dictionary<string, string>() : null;
            string columnList = sqlCreate.GetColumnNameList(Database, Dbo, Dataset, GetPropertyData(), typeof(OBJTYPE), Add: false, Prefix: null, TopMost: true, IdentityName: IdentityName, SiteSpecific: SiteIdentity > 0, WithDerivedInfo: false, SubTable: false,
                VisibleColumns: visibleColumns);

            SQLBuilder sbJoins = new SQLBuilder();
            foreach (JoinData join in Joins) {
                ISQLTableInfo joinInfo = await join.JoinDP.GetDataProvider().GetISQLTableInfoAsync();
                string joinDatabase = joinInfo.GetDatabaseName();
                string joinDbo = joinInfo.GetDbOwner();
                string joinTable = joinInfo.GetTableName();

                ISQLTableInfo mainInfo = await join.MainDP.GetDataProvider().GetISQLTableInfoAsync();
                string mainTable = mainInfo.GetTableName();
                if (join.JoinType == JoinData.JoinTypeEnum.Left)
                    sbJoins.Add($"LEFT JOIN {joinTable}");
                else
                    sbJoins.Add($"INNER JOIN {joinTable}");
                sbJoins.Add(" ON ");
                if (join.UseSite && SiteIdentity > 0)
                    sbJoins.Add("(");
                sbJoins.Add($"{sbJoins.BuildFullColumnName(mainTable, join.MainColumn)} = {sbJoins.BuildFullColumnName(joinTable, join.JoinColumn)}");
                if (join.UseSite && SiteIdentity > 0)
                    sbJoins.Add($") AND {sbJoins.BuildFullColumnName(mainTable, SiteColumn)} = {sbJoins.BuildFullColumnName(joinTable, SiteColumn)}");

                joinTable = joinTable.Split(new char[] { '.' }).Last().Trim(new char[] { '[', ']' });
                List<string> joinCols = sqlManager.GetColumnsOnly(join.JoinDP.GetDataProvider().Conn, joinDatabase, joinDbo, joinTable);
                foreach (string col in joinCols) {
                    if (!visibleColumns.ContainsKey(col)) {
                        string fullCol = sbJoins.BuildFullColumnName(joinDatabase, joinDbo, joinTable, col);
                        visibleColumns.Add(col, fullCol);
                        columnList += $"{fullCol},";
                    }
                }
            }
            string joinExpr = sbJoins.ToString();

            string filterExpr = MakeFilter(sqlHelper, filters, visibleColumns);

            // get total # of records (only if a subset is requested)
            if (skip != 0 || take != 0) {

                sb.Append($@"
SELECT COUNT(*)
FROM {fullTableName} WITH(NOLOCK)
{joinExpr}
{filterExpr}
; --- result set");
            }

            sqlHelper = new SQLHelper(Conn, null, Languages);
            filterExpr = MakeFilter(sqlHelper, filters, visibleColumns);

            string orderByExpr;
            {
                SQLBuilder sbOrder = new SQLBuilder();
                if (sorts == null || sorts.Count == 0)
                    sorts = new List<DataProviderSortInfo> { new DataProviderSortInfo { Field = Key1Name, Order = DataProviderSortInfo.SortDirection.Ascending } };
                sbOrder.AddOrderBy(visibleColumns, sorts, skip, take);
                orderByExpr = sbOrder.ToString();
            }

            // Get records

            if (skip > 0 || take > 0) {
                sb.Append($@"
SELECT * INTO #Temp FROM (");
            }

            sb.Append($@"
    SELECT {columnList}");
            if (CalculatedPropertyCallbackAsync != null) sb.Append(await SQLGen.CalculatedPropertiesAsync(typeof(OBJTYPE), CalculatedPropertyCallbackAsync));
            sb.RemoveLastComma();

            sb.Append($@"
    FROM {fullTableName} WITH(NOLOCK)
    {joinExpr}
    {filterExpr}
    {orderByExpr}");

            if (skip > 0 || take > 0) {
                sb.Append($@"
) data

SELECT * FROM #Temp
;  --- result set");

                filterExpr = string.IsNullOrWhiteSpace(filterExpr) ? "WHERE " : $"{filterExpr} AND ";
                filterExpr += $"[{SQLBase.SubTableKeyColumn}] IN (SELECT [{IdentityNameOrDefault}] FROM #Temp)";
            }

            List<PropertyData> propData = GetPropertyData();
            List<SQLGenericGen.SubTableInfo> subTables = SQLGen.GetSubTables(Dataset, propData);
            foreach (SQLGenericGen.SubTableInfo subTable in subTables) {
                List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                sb.Add($@"
    SELECT [{SQLGenericBase.SubTableKeyColumn}],{sqlCreate.GetColumnNameList(Database, Dbo, subTable.Name, subPropData, subTable.Type, Add: false, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");
                sb.RemoveLastComma();
                sb.Add($@"
    FROM {sb.BuildFullTableName(Database, Dbo, subTable.Name)}
    INNER JOIN {sb.BuildFullTableName(Database, Dbo, Dataset)} ON {sb.BuildFullColumnName(Dataset, IdentityName)} = {sb.BuildFullColumnName(subTable.Name, SQLGenericBase.SubTableKeyColumn)}
    {filterExpr}
;  --- result set");
            }

            if (skip > 0 || take > 0) {
                sb.Append($@"
DROP TABLE #Temp
");
            }

            DataProviderGetRecords<OBJTYPE> recs = new DataProviderGetRecords<OBJTYPE>();

            using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(sb.ToString())) {
                if (skip != 0 || take != 0) {
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync()))
                        return new DataProviderGetRecords<OBJTYPE>();
                    int total = Convert.ToInt32(reader[0]);
                    if (!(YetaWFManager.IsSync() ? reader.NextResult() : await reader.NextResultAsync()))
                        throw new InternalError($"Expected records after count");
                    recs.Total = total;
                }
                while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) {
                    OBJTYPE o = sqlHelper.CreateObject<OBJTYPE>(reader);
                    recs.Data.Add(o);
                }
                if (subTables.Count > 0) {
                    if (!(YetaWFManager.IsSync() ? reader.NextResult() : await reader.NextResultAsync()))
                        throw new InternalError($"Expected additional recordsets");
                    await ReadSubTablesMatchupAsync(sqlHelper, reader, subTables, Dataset, recs.Data, propData, typeof(OBJTYPE));
                }
                if (skip == 0 && take == 0)
                    recs.Total = recs.Data.Count;
            }
            return recs;
        }

        /// <summary>
        /// Removes records using filtering criteria.
        /// </summary>
        /// <param name="filters">A collection describing the filtering criteria.</param>
        /// <returns>Returns the number of records removed.</returns>
        public async Task<int> RemoveRecordsAsync(List<DataProviderFilterInfo> filters) {

            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            SQLBuilder sb = new SQLBuilder();

            filters = NormalizeFilter(typeof(OBJTYPE), filters);

            string fullTableName = sb.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = GetPropertyData();
            string filterExpr = MakeFilter(sqlHelper, filters);

            List<SQLGenericGen.SubTableInfo> subTables = SQLGen.GetSubTables(Dataset, propData);
            foreach (SQLGenericGen.SubTableInfo subTable in subTables) {
                string subFilters;
                if (string.IsNullOrWhiteSpace(filterExpr))
                    subFilters = $@"WHERE {sb.GetTable(Database, Dbo, subTable.Name)}.[{SQLGenericBase.SubTableKeyColumn}] = {fullTableName}.[{IdentityNameOrDefault}]";
                else
                    subFilters = $@"{filterExpr} AND {sb.GetTable(Database, Dbo, subTable.Name)}.[{SQLGenericBase.SubTableKeyColumn}] = {fullTableName}.[{IdentityNameOrDefault}]";

                sb.Append($@"
DELETE FROM {sb.GetTable(Database, Dbo, subTable.Name)} 
LEFT JOIN {fullTableName} ON {sb.BuildFullColumnName(Dataset, IdentityName)} = {sb.BuildFullColumnName(subTable.Name, SQLGenericBase.SubTableKeyColumn)}
{subFilters}
;
");

            }

            sb.Append($@"
DELETE
FROM {fullTableName}
{filterExpr}
;

    SELECT @@ROWCOUNT --- result set
");

            object val = await sqlHelper.ExecuteScalarAsync(sb.ToString());
            int deleted = Convert.ToInt32(val);
            return deleted;
        }

        internal async Task ReadSubTablesAsync(SQLHelper sqlHelper, SqlDataReader reader, string tableName, OBJTYPE container, Type tpContainer) {

            List<SQLGenericGen.SubTableInfo> subTables = SQLGen.GetSubTables(tableName, GetPropertyData());
            foreach (SQLGenericGen.SubTableInfo subTable in subTables) {
                object subContainer = subTable.PropInfo.GetValue(container);
                if (subContainer == null) throw new InternalError($"{nameof(ReadSubTablesAsync)} encountered an enumeration property that is null");

                // find the Add method for the collection so we can add each item as its read
                MethodInfo addMethod = subTable.PropInfo.PropertyType.GetMethod("Add", new Type[] { subTable.Type });
                if (addMethod == null) throw new InternalError($"{nameof(ReadSubTablesAsync)} encountered an enumeration property that doesn't have an Add method");

                if (!(YetaWFManager.IsSync() ? reader.NextResult() : await reader.NextResultAsync())) throw new InternalError("Expected next result set (subtable)");
                while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) {
                    object obj = sqlHelper.CreateObject(reader, subTable.Type);
                    addMethod.Invoke(subContainer, new object[] { obj });
                }
            }
        }

        internal async Task ReadSubTablesMatchupAsync(SQLHelper sqlHelper, SqlDataReader reader, List<SQLGenericGen.SubTableInfo> subTables, string tableName, List<OBJTYPE> containers, List<PropertyData> propData, Type tpContainer) {

            // extract identities from container list so we can match sub-objects more easily
            List<int> identities = GetIdentities(containers);

            for (; ; ) {
                foreach (SQLGenericGen.SubTableInfo subTable in subTables) {

                    while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) {

                        // find the Add method for the collection so we can add each item as its read
                        MethodInfo addMethod = subTable.PropInfo.PropertyType.GetMethod("Add", new Type[] { subTable.Type });
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
                int identity = (int)piIdent.GetValue(c);
                list.Add(identity);
            }
            return list;
        }

        private void AddToContainer(List<OBJTYPE> containers, List<int> identities, SQLGenericGen.SubTableInfo subTable, object obj, int key, MethodInfo addMethod) {

            int index = identities.IndexOf(key); // find the index of the matching identity/container
            if (index < 0) throw new InternalError($"Subtable {subTable.Name} has key {key} that doesn't match any main record");

            OBJTYPE container = containers[index];
            object subContainer = subTable.PropInfo.GetValue(container);
            if (subContainer == null) throw new InternalError($"{nameof(AddToContainer)} encountered a enumeration property that is null");

            addMethod.Invoke(subContainer, new object[] { obj });
        }

        // IINSTALLABLEMODEL
        // IINSTALLABLEMODEL
        // IINSTALLABLEMODEL

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
            bool success = sqlCreate.CreateTableFromModel(Database, Dbo, Dataset, Key1Name, HasKey2 ? Key2Name : null, IdentityName, GetPropertyData(), typeof(OBJTYPE), errorList,
                SiteSpecific: SiteIdentity > 0,
                TopMost: true);

            // update cache
            SQLGenericManagerCache.ClearCache();
            SQLManager sqlManager = new SQLManager();
            sqlManager.GetColumns(Conn, Database, Dbo, Dataset);

            sqlCreate.MakeTypes(Database, Dbo, Dataset, GetPropertyData(), typeof(OBJTYPE));

            if (success) {
                if (!await sqlCreate.MakeFunctionsAsync(Database, Dbo, Dataset, Key1Name, HasKey2 ? Key2Name : null, IdentityName, GetPropertyData(), typeof(OBJTYPE), SiteIdentity, CalculatedPropertyCallbackAsync))
                    success = false;
            }
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
#pragma warning disable 1734 // ignore getRecordsAsync, saveRecordAsync
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
        /// 
        /// The YetaWF.Core.Models.ObjectSupport.TranslateObject method is to translate all YetaWF.Core.Models.MultiString instances.
        ///
        /// The method providing <paramref name="getRecordsAsync"/> and <paramref name="saveRecordAsync"/> methods is used by derived classes to translate the data managed by the data provider to another language.
        /// The derived class provides <paramref name="getRecordsAsync"/> and <paramref name="saveRecordAsync"/> methods, which are used to retrieve, translate and save the data.
        /// </remarks>
#pragma warning restore 1734
        public async Task LocalizeModelAsync(string language, Func<string, bool> isHtml, Func<List<string>, Task<List<string>>> translateStringsAsync, Func<string, Task<string>> translateComplexStringAsync) {
            await EnsureOpenAsync();
            await LocalizeModelAsync(language, isHtml, translateStringsAsync, translateComplexStringAsync,
                async (int offset, int skip) => {
                    return await GetRecordsAsync(offset, skip, null, null);
                },
                async (OBJTYPE record, PropertyInfo pi, PropertyInfo pi2) => {
                    UpdateStatusEnum status;
                    KEYTYPE key1 = (KEYTYPE)pi.GetValue(record);
                    Warnings = false; // we're turning warnings off in case strings get truncated
                    try {
                        if (HasKey2) {
                            KEYTYPE2 key2 = (KEYTYPE2)pi2.GetValue(record);
                            status = await UpdateAsync(key1, key2, key1, key2, record);
                        } else {
                            status = await UpdateAsync(key1, key1, record);
                        }
                    } catch (Exception) {
                        throw;
                    } finally {
                        Warnings = true;// turn warnings back on
                    }
                    return status;
                });
        }
#pragma warning disable 1734 // ignore getRecordsAsync, saveRecordAsync
        /// <summary>
        /// Called to translate the data managed by the data provider to another language.
        /// </summary>
        /// <param name="language">The target language (see LanguageSettings.json).</param>
        /// <param name="isHtml">A method that can be called by the data provider to test whether a string contains HTML.</param>
        /// <param name="translateStringsAsync">A method that can be called to translate a collection of simple strings into the new language. A simple string does not contain HTML or newline characters.</param>
        /// <param name="translateComplexStringAsync">A method that can be called to translate a collection of complex strings into the new language. A complex string can contain HTML and newline characters.</param>
        /// <param name="getRecordsAsync">Used by derived classes to retrieve the records to translate.</param>
        /// <param name="saveRecordAsync">Used by derived classes to save the translated records.</param>
        /// <remarks>
        /// The data provider has to retrieve all records and translate them as needed using the
        /// provided <paramref name="translateStringsAsync"/> and <paramref name="translateComplexStringAsync"/> methods, and save the translated data.
        ///
        /// The YetaWF.Core.Models.ObjectSupport.TranslateObject method can be used to translate all YetaWF.Core.Models.MultiString instances.
        ///
        /// The translated data should be stored separately from the default language (except MultiString, which is part of the record).
        /// Using the <paramref name="language"/> parameter, a different folder should be used to store the translated data.
        /// 
        /// The YetaWF.Core.Models.ObjectSupport.TranslateObject method is to translate all YetaWF.Core.Models.MultiString instances.
        ///
        /// The method providing <paramref name="getRecordsAsync"/> and <paramref name="saveRecordAsync"/> methods is used by derived classes to translate the data managed by the data provider to another language.
        /// The derived class provides <paramref name="getRecordsAsync"/> and <paramref name="saveRecordAsync"/> methods, which are used to retrieve, translate and save the data.
        /// </remarks>
#pragma warning restore 1734
        protected async Task LocalizeModelAsync(string language,
                Func<string, bool> isHtml,
                Func<List<string>, Task<List<string>>> translateStringsAsync, Func<string, Task<string>> translateComplexStringAsync, Func<int, int, Task<DataProviderGetRecords<OBJTYPE>>> getRecordsAsync, Func<OBJTYPE, PropertyInfo, PropertyInfo, Task<UpdateStatusEnum>> saveRecordAsync) {

            const int RECORDS = 20;

            List<PropertyInfo> props = ObjectSupport.GetProperties(typeof(OBJTYPE));
            PropertyInfo key1Prop = ObjectSupport.GetProperty(typeof(OBJTYPE), Key1Name);
            PropertyInfo key2Prop = null;
            if (HasKey2)
                key2Prop = ObjectSupport.GetProperty(typeof(OBJTYPE), Key2Name);

            for (int offset = 0; ;) {
                DataProviderGetRecords<OBJTYPE> data = await getRecordsAsync(offset, RECORDS);
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

        // Helpers

        internal string GetParameterList(SQLHelper sqlHelper, OBJTYPE obj, string dbName, string schema, string dataset, List<PropertyData> propData, string Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            SQLManager sqlManager = new SQLManager();
            return SQLGen.ProcessColumns(
                (prefix, container, prop) => { // Property
                    SQLGenericGen.Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    object val = prop.PropInfo.GetValue(container);
                    sqlHelper.AddParam($"arg{prefix}{prop.Name}", val, DbType: (SqlDbType)col.DataType);
                    return null;
                },
                (prefix, container, prop) => { // Identity
                    return null;
                },
                (prefix, container, prop) => { // Binary
                    SQLGenericGen.Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    object val = prop.PropInfo.GetValue(container);
                    byte[] data = null;
                    if (val != null) {
                        PropertyInfo pi = prop.PropInfo;
                        if (pi.PropertyType == typeof(byte[])) {
                            data = (byte[])val;
                        } else {
                            data = new GeneralFormatter().Serialize(val);
                        }
                    }
                    sqlHelper.AddParam($"arg{prefix}{prop.Name}", data, DbType: SqlDbType.Binary);
                    return null;
                },
                (prefix, container, prop) => { // Image
                    SQLGenericGen.Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    object val = prop.PropInfo.GetValue(container);
                    sqlHelper.AddParam($"arg{prefix}{prop.Name}", val, DbType: (SqlDbType)col.DataType);
                    return null;
                },
                (prefix, container, prop) => { // Language
                    MultiString ms = (MultiString)prop.PropInfo.GetValue(container);
                    foreach (LanguageData lang in Languages) {
                        string colName = ColumnFromPropertyWithLanguage(lang.Id, prop.ColumnName);
                        SQLGenericGen.Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{colName}");
                        sqlHelper.AddParam($"arg{prefix}{col.Name}", ms[lang.Id] ?? "", DbType: SqlDbType.NVarChar);
                    }
                    return null;
                },
                (prefix, container, name) => { // predef
                    if (name == SQLGenericBase.SiteColumn)
                        sqlHelper.AddParam(SQLGen.ValSiteIdentity, SiteIdentity, DbType: SqlDbType.Int);
                    return null;
                },
                (prefix, container, prop, subPropData, subType, subtableName) => { // Subtable

                    GetSubtableParm(sqlHelper, dbName, schema, subtableName, prefix, container, prop, subPropData, subType);
                    return null;
                },
                dbName, schema, dataset, obj, propData, obj.GetType(), Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }

        private void GetSubtableParm(SQLHelper sqlHelper, string dbName, string schema, string subtableName, string prefix, object container, PropertyData prop, List<PropertyData> subPropData, Type subType) {

            // https://stackoverflow.com/questions/11102358/how-to-pass-an-array-into-a-sql-server-stored-procedure
            // https://stackoverflow.com/questions/7595416/convert-dictionarystring-object-to-anonymous-object

            // Extract all column names

            DataTable dataTable = new DataTable();

            SQLGen.ProcessColumns(
                (prefix, container, prop) => { // Property
                    string colName = $"{prefix}{prop.ColumnName}";
                    dataTable.Columns.Add(new DataColumn(colName, prop.PropInfo.PropertyType));
                    return null;
                },
                (prefix, container, prop) => { // Identity
                    return null;
                },
                (prefix, container, prop) => { // Binary
                    string colName = $"{prefix}{prop.ColumnName}";
                    dataTable.Columns.Add(new DataColumn(colName, prop.PropInfo.PropertyType));
                    return null;
                },
                (prefix, container, prop) => { // Image
                    string colName = $"{prefix}{prop.ColumnName}";
                    dataTable.Columns.Add(new DataColumn(colName, prop.PropInfo.PropertyType));
                    return null;
                },
                (prefix, container, prop) => { // Language
                    MultiString ms = (MultiString)prop.PropInfo.GetValue(container);
                    foreach (LanguageData lang in Languages) {
                        string colName = ColumnFromPropertyWithLanguage(lang.Id, prop.ColumnName);
                        dataTable.Columns.Add(new DataColumn(colName, typeof(string)));
                    }
                    return null;
                },
                (prefix, container, name) => { // predef
                    return null;
                },
                (prefix, container, prop, subPropData, subType, subtableName) => { // Subtable
                    throw new InternalError($"Nested subtables not supported");
                },

                dbName, schema, subtableName, container, subPropData, container.GetType(), Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true);

            // Get all rows

            List<object> list = new List<object>();
            object val = prop.PropInfo.GetValue(container);
            if (val == null) {
                sqlHelper.AddParam($"arg{prefix}{prop.Name}", null);
                return;
            }
            list = new List<object>((IEnumerable<object>)val);
            foreach (object entry in list) {

                DataRow row = dataTable.NewRow();
                GetSubtableParmValues(sqlHelper, row, dbName, schema, subtableName, prefix, entry, prop, subPropData, subType);
                dataTable.Rows.Add(row);
            }

            sqlHelper.AddParam($"arg{prefix}{prop.Name}", dataTable, DbType: SqlDbType.Structured, DataTypeName: $"{subtableName}_T");
        }
        private void GetSubtableParmValues(SQLHelper sqlHelper, DataRow row, string dbName, string schema, string subtableName, string prefix, object container, PropertyData prop, List<PropertyData> subPropData, Type subType) {
            SQLGen.ProcessColumns(
                (prefix, container, prop) => { // Property
                    row[$"{prefix}{prop.ColumnName}"] = prop.PropInfo.GetValue(container);
                    return null;
                },
                (prefix, container, prop) => { // Identity
                    return null;
                },
                (prefix, container, prop) => { // Binary
                    object val = prop.PropInfo.GetValue(container);
                    byte[] data = null;
                    if (val != null) {
                        PropertyInfo pi = prop.PropInfo;
                        if (pi.PropertyType == typeof(byte[])) {
                            data = (byte[])val;
                        } else {
                            data = new GeneralFormatter().Serialize(val);
                        }
                    }
                    row[$"{prefix}{prop.ColumnName}"] = data;
                    return null;
                },
                (prefix, container, prop) => { // Image
                    row[$"{prefix}{prop.ColumnName}"] = prop.PropInfo.GetValue(container);
                    return null;
                },
                (prefix, container, prop) => { // Language
                    MultiString ms = (MultiString)prop.PropInfo.GetValue(container);
                    foreach (LanguageData lang in Languages) {
                        string colName = ColumnFromPropertyWithLanguage(lang.Id, prop.ColumnName);
                        row[$"{prefix}{colName}"] = ms[lang.Id] ?? "";
                    }
                    return null;
                },
                (prefix, container, name) => { // predef
                    return null;
                },
                (prefix, container, prop, subPropData, subType, subtableName) => { // Subtable
                    throw new InternalError($"Nested subtables not supported");
                },
                dbName, schema, subtableName, container, subPropData, container.GetType(), Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true);
        }
    }
}
