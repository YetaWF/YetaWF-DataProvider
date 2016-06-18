/* Copyright © 2016 Softel vdm, Inc. - http://yetawf.com/Documentation/YetaWF/Licensing */

using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;

// This SQL data provider doesn't offer specific methods for access by identity in order to make it compatible with the File data provider
// For full identity support use SQLIdentityObject instead

namespace YetaWF.DataProvider {
    public partial class SQLSimpleObjectDataProvider<KEYTYPE, OBJTYPE> : SQLDataProviderImpl, IDataProvider<KEYTYPE, OBJTYPE>
    {
        private const int ChunkSize = 100;

        public SQLSimpleObjectDataProvider(string table, string dbOwner, string connString, int dummy = 0,
                int CurrentSiteIdentity = 0,
                bool NoLanguages = false,
                bool Cacheable = false,
                bool Logging = true,
                int IdentitySeed = 0,
                Func<string, string> CalculatedPropertyCallback = null)
            : base(dbOwner, connString, table, Logging, NoLanguages, Cacheable, CurrentSiteIdentity, IdentitySeed, CalculatedPropertyCallback) {
            this.UseIdentity = !string.IsNullOrWhiteSpace(IdentityName);
        }

        public string Key1Name { get { return GetKey1Name(TableName, GetPropertyData(typeof(OBJTYPE))); } }
        public string IdentityName { get { return GetIdentityName(TableName, GetPropertyData(typeof(OBJTYPE))); } }

        protected List<PropertyData> GetPropertyData() {
            return GetPropertyData(typeof(OBJTYPE));
        }
        public string ReplaceWithTableName(string text, string searchText) { return text.Replace(searchText, GetTableName()); }
        public string GetTableName() { return string.Format("[{0}].[{1}].[{2}]", DatabaseName, DbOwner, TableName); }

        public OBJTYPE Get(KEYTYPE key) {
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);

                DB.SELECT("TOP 1 *");
                AddCalculatedProperties(DB, typeof(OBJTYPE));
                DB.FROM(TableName);
                MakeJoins(DB, null);
                DB.WHERE(Key1Name, key);
                if (CurrentSiteIdentity > 0)
                    DB.AND(SiteColumn, CurrentSiteIdentity);

                OBJTYPE obj = DB.ExecuteObject<OBJTYPE>();
                if (obj != null)
                    ReadSubTables(DB, TableName, IdentityName, GetPropertyData(), obj, typeof(OBJTYPE));
                return obj;
            }
        }
        public UpdateStatusEnum Update(KEYTYPE origKey, KEYTYPE newKey, OBJTYPE obj) {
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                using (SqlTransaction tr = connection.BeginTransaction()) {
                    BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, tr, Languages);
                    DB.UPDATE(TableName);
                    AddSetColumns(DB, TableName, IdentityName, GetPropertyData(), obj, typeof(OBJTYPE));
                    DB.WHERE(Key1Name, origKey);
                    if (CurrentSiteIdentity > 0)
                        DB.AND(SiteColumn, CurrentSiteIdentity);
                    DB.SELECT("@@ROWCOUNT");
                    try {
                        int changed = DB.ExecuteScalarInt();
                        if (changed == 0)
                            return UpdateStatusEnum.RecordDeleted;
                        if (changed > 1)
                            throw new InternalError("Update failed - {0} records updated", changed);
                    } catch (Exception exc) {
                        if (!newKey.Equals(origKey)) {
                            SqlException sqlExc = exc as SqlException;
                            if (sqlExc != null && sqlExc.Number == 2627) {
                                // duplicate key violation, meaning the new key already exists
                                return UpdateStatusEnum.NewKeyExists;
                            }
                        }
                        throw new InternalError("Update failed for type {0} - {1}", typeof(OBJTYPE).FullName, exc.Message);
                    }
                    tr.Commit();
                }
            }
            return UpdateStatusEnum.OK;
        }
        public bool Add(OBJTYPE obj) {
            subDBs = new List<BigfootSQL.SqlHelper>();
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                using (SqlTransaction tr = connection.BeginTransaction()) {
                    BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, tr, Languages);
                    DB.INSERTINTO(TableName, GetColumnList(DB, GetPropertyData(), obj.GetType(), "", true, SiteSpecific: CurrentSiteIdentity > 0))
                            .VALUES(GetValueList(DB, TableName, GetPropertyData(), obj, typeof(OBJTYPE), SiteSpecific: CurrentSiteIdentity > 0));
                    int identity = 0;
                    try {
                        if (UseIdentity)
                            identity = DB.ExecuteScalarIdentity();
                        else
                            DB.ExecuteNonquery();
                    } catch (Exception exc) {
                        SqlException sqlExc = exc as SqlException;
                        if (sqlExc != null && sqlExc.Number == 2627) // already exists
                            return false;
                        throw new InternalError("Add failed for type {0} - {1}", typeof(OBJTYPE).FullName, exc.Message);
                    }

                    if (subDBs.Count > 0) {
                        if (!UseIdentity)
                            throw new InternalError("A subtable was encountered but the main table {0} doesn't use an identity", TableName);
                        foreach (BigfootSQL.SqlHelper db in subDBs) {
                            db.AddParam("__Identity", identity);
                            db.ExecuteNonquery();
                        }
                    }
                    tr.Commit();

                    if (UseIdentity) {
                        PropertyInfo piIdent = ObjectSupport.GetProperty(typeof(OBJTYPE), IdentityName);
                        if (piIdent.PropertyType != typeof(int)) throw new InternalError("SQLSimpleObject only supports object identities of type int");
                        piIdent.SetValue(obj, identity);
                    }
                }
            }
            return true;
        }
        public bool Remove(KEYTYPE key) {
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);
                DB.DELETEFROM(TableName);
                DB.WHERE(Key1Name, key);
                if (CurrentSiteIdentity > 0)
                    DB.AND(SiteColumn, CurrentSiteIdentity);
                DB.SELECT("@@ROWCOUNT");
                int deleted = DB.ExecuteScalarInt();
                if (deleted > 1) throw new InternalError("More than 1 record deleted by Remove() method");
                return deleted > 0;
            }
        }
        public int RemoveRecords(List<DataProviderFilterInfo> filters) {
            filters = NormalizeFilter(typeof(OBJTYPE), filters);
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);
                DB.DELETEFROM(TableName);
                MakeFilter(DB, filters);
                DB.SELECT("@@ROWCOUNT");
                int deleted = DB.ExecuteScalarInt();
                return deleted;
            }
        }

        public List<KEYTYPE> GetKeyList() {
            throw new InternalError("Not implemented");
        }

        public List<OBJTYPE> GetRecords(int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, out int total, List<JoinData> Joins = null) {
            filters = NormalizeFilter(typeof(OBJTYPE), filters);
            sort = NormalizeSort(typeof(OBJTYPE), sort);
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);
                List<OBJTYPE> serList = GetMainTableRecords(DB, skip, take, sort, filters, out total, Joins: Joins);
                return serList;
            }
        }
        public OBJTYPE GetOneRecord(List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            filters = NormalizeFilter(typeof(OBJTYPE), filters);
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);
                int total;
                OBJTYPE obj = GetMainTableRecords(DB, 0, 1, null, filters, out total, Joins: Joins).FirstOrDefault();
                return obj;
            }
        }

        // DIRECT QUERY
        // DIRECT QUERY
        // DIRECT QUERY

        public List<OBJTYPE> Direct_QueryList(string sql) {
            return base.Direct_QueryList<OBJTYPE>(TableName, sql);
        }

        public int Direct_ScalarInt(string sql) {
            return base.Direct_ScalarInt(TableName, sql);
        }
        public void Direct_Query(string sql) {
             base.Direct_Query(TableName, sql);
        }

        private List<OBJTYPE> GetMainTableRecords(BigfootSQL.SqlHelper DB, int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, out int total,
                 List<JoinData> Joins = null) {

            total = 0;
            // get total # of records (only if a subset is requested)
            Dictionary<string, string> visibleColumns = null;
            if (skip != 0 || take != 0) {
                visibleColumns = GetVisibleColumns(GetDatabase(DB.SqlConnection), DatabaseName, DbOwner, TableName, typeof(OBJTYPE), Joins);
                total = 0;
                DB.Clear();
                DB.SELECT("COUNT(*)").FROM(TableName);
                MakeJoins(DB, Joins);
                MakeFilter(DB, filters, visibleColumns);
                total = DB.ExecuteScalarInt();
            }

            if (visibleColumns == null)
                visibleColumns = GetVisibleColumns(GetDatabase(DB.SqlConnection), DatabaseName, DbOwner, TableName, typeof(OBJTYPE), Joins);
            DB.Clear();
            DB.SELECT("*");
            AddCalculatedProperties(DB, typeof(OBJTYPE));

            DB.FROM(TableName);
            MakeJoins(DB, Joins);
            MakeFilter(DB, filters, visibleColumns);

            if (sort == null || sort.Count() == 0)
                sort = new List<DataProviderSortInfo> { new DataProviderSortInfo { Field = Key1Name, Order = DataProviderSortInfo.SortDirection.Ascending } };
            DB.ORDERBY(visibleColumns, sort, Offset: skip, Next: take);// we can sort by all fields, including joined tables
            List<OBJTYPE> list = DB.ExecuteCollection<OBJTYPE>();
            if (skip == 0 && take == 0)
                total = list.Count();
            ReadSubTableRecords(DB, list);
            return list;
        }

        private void ReadSubTableRecords(BigfootSQL.SqlHelper DB, List<OBJTYPE> list) {
            foreach (var obj in list) {
                DB.Clear();
                ReadSubTables(DB, TableName, IdentityName, GetPropertyData(), obj, typeof(OBJTYPE));
            }
        }

        public bool IsInstalled() {
            if (_IsInstalled == null) {
                using (SqlConnection conn = new SqlConnection(ConnString)) {
                    Database db = GetDatabase(conn);
                    _IsInstalled = db.Tables.Contains(TableName);
                }
            }
            return (bool)_IsInstalled;
        }
        public bool? _IsInstalled { get; set; }

        public bool InstallModel(List<string> errorList) {
            bool success = false;
            using (SqlConnection conn = new SqlConnection(ConnString)) {
                Database db = GetDatabase(conn);
                List<string> columns = new List<string>();
                success = CreateTable(conn, db, TableName, Key1Name, null, IdentityName, GetPropertyData(), typeof(OBJTYPE), errorList, columns,
                    SiteSpecific: CurrentSiteIdentity > 0,
                    TopMost: true, UseIdentity: UseIdentity);
            }
            _IsInstalled = true;
            return success;
        }
        public bool UninstallModel(List<string> errorList) {
            try {
                _IsInstalled = false;
                using (SqlConnection conn = new SqlConnection(ConnString)) {
                    Database db = GetDatabase(conn);
                    DropSubTables(conn, db, TableName, errorList);
                    DropTable(conn, db, TableName, errorList);
                }
                return true;
            } catch (Exception exc) {
                errorList.Add(string.Format("{0}: {1}", typeof(OBJTYPE).FullName, exc.Message));
                return false;
            }
        }
        public void AddSiteData() { }
        public void RemoveSiteData() { // remove site-specific data
            if (CurrentSiteIdentity > 0) {
                using (SqlConnection connection = new SqlConnection(ConnString)) {
                    connection.Open();
                    BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);
                    DB.DELETEFROM(TableName).WHERE(SiteColumn, CurrentSiteIdentity).ExecuteScalar();
                }
            }
        }
        public bool ExportChunk(int chunk, SerializableList<SerializableFile> fileList, out object obj) {
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);

                DB.SELECT("*");
                DB.FROM(DatabaseName, DbOwner, TableName);
                MakeJoins(DB, null);
                MakeFilter(DB, null);

                List<DataProviderSortInfo> sort = new List<DataProviderSortInfo> { new DataProviderSortInfo { Field = Key1Name, Order = DataProviderSortInfo.SortDirection.Ascending } };
                DB.ORDERBY(null, sort, Offset: chunk * ChunkSize, Next: ChunkSize);
                List<OBJTYPE> list = DB.ExecuteCollection<OBJTYPE>();
                ReadSubTableRecords(DB, list);
                SerializableList<OBJTYPE> serList = new SerializableList<OBJTYPE>(list);
                obj = serList;
                int count = serList.Count();
                if (count == 0)
                    obj = null;
                return (count >= ChunkSize);
            }
        }
        public void ImportChunk(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            if (CurrentSiteIdentity > 0 || YetaWFManager.Manager.ImportChunksNonSiteSpecifics) {
                SerializableList<OBJTYPE> serList = (SerializableList<OBJTYPE>)obj;
                int total = serList.Count();
                if (total > 0) {
                    using (SqlConnection connection = new SqlConnection(ConnString)) {
                        connection.Open();
                        for (int processed = 0 ; processed < total ; ++processed) {
                            using (SqlTransaction tr = connection.BeginTransaction()) {
                                subDBs = new List<BigfootSQL.SqlHelper>();
                                BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, tr, Languages);
                                OBJTYPE item = serList[processed];
                                DB.INSERTINTO(TableName, GetColumnList(DB, GetPropertyData(), typeof(OBJTYPE), "", true, SiteSpecific: CurrentSiteIdentity > 0))
                                        .VALUES(GetValueList(DB, TableName, GetPropertyData(), item, typeof(OBJTYPE), SiteSpecific: CurrentSiteIdentity > 0));
                                int identity = 0;
                                if (UseIdentity)
                                    identity = DB.ExecuteScalarIdentity();
                                else
                                    DB.ExecuteNonquery();
                                if (subDBs.Count > 0) {
                                    if (!UseIdentity)
                                        throw new InternalError("A subtable was encountered but the main table {0} doesn't use an identity", TableName);
                                    foreach (BigfootSQL.SqlHelper db in subDBs) {
                                        db.AddParam("__Identity", identity);
                                        db.ExecuteNonquery();
                                    }
                                }
                                tr.Commit();
                            }
                        }
                    }
                }
            }
        }
    }
}
