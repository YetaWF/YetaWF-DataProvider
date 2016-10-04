﻿/* Copyright © 2016 Softel vdm, Inc. - http://yetawf.com/Documentation/YetaWF/Licensing */

using BigfootSQL;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider
{
    public partial class SQLDerivedObjectDataProvider<KEYTYPE, OBJTYPE, BASEOBJTYPE> : SQLDataProviderImpl, IDataProvider<KEYTYPE, OBJTYPE>
    {
        private const int ChunkSize = 100;

        public SQLDerivedObjectDataProvider(string table, string dbOwner, string connString,
                string baseTableName,
                int dummy = 0,
                int CurrentSiteIdentity = 0,
                bool NoLanguages = false,
                bool Cacheable = false, bool Logging = true) : base(dbOwner, connString, table, Logging, NoLanguages, Cacheable, CurrentSiteIdentity) {
            BaseTableName = baseTableName;
            UseIdentity = !string.IsNullOrWhiteSpace(IdentityName);
        }

        public string BaseTableName { get; private set; }

        public string Key1Name { get { return GetKey1Name(TableName, GetPropertyData(typeof(OBJTYPE))); } }
        public string IdentityName { get { return GetIdentityName(TableName, GetPropertyData(typeof(OBJTYPE))); } }

        protected List<PropertyData> GetBasePropertyData() {
            return GetPropertyData(typeof(BASEOBJTYPE));
        }

        protected List<PropertyData> GetPropertyData() {
            if (_propertyData == null) {
                List<PropertyData> propData = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
                // subtract all properties that are already defined in the base type
                List<PropertyData> basePropData = ObjectSupport.GetPropertyData(typeof(BASEOBJTYPE));
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

        public string ReplaceWithTableName(string text, string searchText) { return text.Replace(searchText, GetTableName()); }
        public string GetTableName() { return string.Format("[{0}].[{1}].[{2}]", DatabaseName, DbOwner, TableName); }

        public OBJTYPE Get(KEYTYPE key) {
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);
                if (BaseTableName == TableName) {
                    // we're reading the generic type and have to find the derived type
                    DB.SELECT("DerivedDataTableName", "DerivedDataType", "DerivedAssemblyName").FROM(BaseTableName);
                    DB.WHERE(Key1Name, key);
                    if (CurrentSiteIdentity > 0)
                        DB.AND(SiteColumn, CurrentSiteIdentity);
                    SqlDataReader reader = DB.ExecuteReader();
                    string derivedTableName = null, derivedDataType = null, derivedAssemblyName = null;
                    if (reader.Read()) {
                        derivedTableName = (string) reader[0];
                        derivedDataType = (string) reader[1];
                        derivedAssemblyName = (string) reader[2];
                    }
                    reader.Close();
                    if (string.IsNullOrWhiteSpace(derivedTableName))
                        return default(OBJTYPE);

                    // We have the table and the type
                    // instantiate the right type object
                    Type t = null;
                    object obj = null;
                    try {
                        Assembly asm = Assemblies.Load(derivedAssemblyName);
                        t = asm.GetType(derivedDataType, true);
                        obj = Activator.CreateInstance(t);
                    } catch (Exception exc) {
                        throw new InternalError("Invalid Type {0} requested from table {1} - {2} - {3}.", derivedDataType, derivedTableName, derivedAssemblyName, exc.Message);
                    }
                    // Now read all data for this derived and base type
                    DB.Clear();
                    DB.SELECT("TOP 1 *")
                        .FROM(derivedTableName)
                        .INNERJOIN(BaseTableName).ON(BaseTableName + "." + Key1Name, derivedTableName + "." + Key1Name);
                    if (CurrentSiteIdentity > 0)
                        DB.ANDON(BaseTableName + "." + SiteColumn, derivedTableName + "." + SiteColumn);
                    DB.WHERE(derivedTableName + "." + Key1Name, key);
                    if (CurrentSiteIdentity > 0)
                        DB.AND(derivedTableName + "." + SiteColumn, CurrentSiteIdentity);
                    reader = DB.ExecuteReader();
                    // fill the object with the data
                    if (reader.Read()) {
                        ObjectHelper objHelper = new ObjectHelper(Languages);
                        objHelper.FillObject(reader, obj);
                    } else
                        obj = default(OBJTYPE);
                    reader.Close();
                    return (OBJTYPE) obj;
                } else {
                    // We have all info - just read the derived and base type info and create the object
                    DB.SELECT("TOP 1 *")
                        .FROM(TableName)
                        .INNERJOIN(BaseTableName).ON(BaseTableName + "." + Key1Name, TableName + "." + Key1Name);
                    if (CurrentSiteIdentity > 0)
                        DB.ANDON(BaseTableName + "." + SiteColumn, TableName + "." + SiteColumn);
                    DB.WHERE(TableName + "." + Key1Name, key);
                    if (CurrentSiteIdentity > 0)
                        DB.AND(TableName + "." + SiteColumn, CurrentSiteIdentity);
                    OBJTYPE obj = DB.ExecuteObject<OBJTYPE>();
                    return obj;
                }
            }
        }

        public UpdateStatusEnum Update(KEYTYPE origKey, KEYTYPE newKey, OBJTYPE obj) {
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                using (SqlTransaction tr = connection.BeginTransaction()) {
                    BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, tr, Languages);
                    Type baseType = typeof(BASEOBJTYPE);
                    DB.UPDATE(BaseTableName);
                    AddSetColumns(DB, TableName, IdentityName, GetBasePropertyData(), obj, baseType);
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

                    DB.Clear();
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
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                using (SqlTransaction tr = connection.BeginTransaction()) {
                    BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, tr, Languages);
                    Type baseType = typeof(BASEOBJTYPE);
                    subDBs = new List<BigfootSQL.SqlHelper>();
                    DB.INSERTINTO(BaseTableName, GetColumnList(DB, GetBasePropertyData(), baseType, "", true, SiteSpecific: CurrentSiteIdentity > 0, WithDerivedInfo: true))
                        .VALUES(GetValueList(DB, BaseTableName, GetBasePropertyData(), obj, baseType, DerivedType: typeof(OBJTYPE), SiteSpecific: CurrentSiteIdentity > 0, DerivedTableName: TableName));
                    if (subDBs.Count > 0)
                        throw new InternalError("Subtable not supported for table {0}", TableName);

                    int identity = 0;
                    try {
                        if (UseIdentity) {
                            identity = DB.ExecuteScalarIdentity();
                            if (identity == 0) throw new InternalError("Insert failed for type {0}", baseType.FullName);
                        } else
                            DB.ExecuteNonquery();
                    } catch (Exception exc) {
                        SqlException sqlExc = exc as SqlException;
                        if (sqlExc != null && sqlExc.Number == 2627) // already exists
                            return false;
                        throw new InternalError("Add failed for type {0} - {1}", typeof(OBJTYPE).FullName, exc.Message);
                    }

                    DB.Clear();
                    DB.INSERTINTO(TableName, GetColumnList(DB, GetPropertyData(), obj.GetType(), "", true, SiteSpecific: CurrentSiteIdentity > 0))
                        .VALUES(GetValueList(DB, TableName, GetPropertyData(), obj, typeof(OBJTYPE), SiteSpecific: CurrentSiteIdentity > 0))
                        .ExecuteNonquery();
                    if (subDBs.Count > 0)
                        throw new InternalError("Subtable not supported for table {0}", TableName);

                    tr.Commit();

                    if (UseIdentity) {
                        PropertyInfo piIdent = ObjectSupport.TryGetProperty(typeof(OBJTYPE), IdentityName);
                        if (piIdent == null) throw new InternalError("Type {0} has no identity property named {1}", typeof(OBJTYPE).FullName, IdentityName);
                        if (piIdent.PropertyType != typeof(int)) throw new InternalError("SQLDerivedObject only supports object identities of type int");
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
        public List<OBJTYPE> GetRecords(int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, out int total, List<JoinData> Join = null) {
            // IMPORTANT: THIS ONLY SUPPORTS THE PRIMARY (non-derived) DATA
            filters = NormalizeFilter(typeof(OBJTYPE), filters);
            sort = NormalizeSort(typeof(OBJTYPE), sort);
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);
                List<OBJTYPE> serList = GetMainTableRecords(DB, skip, take, sort, filters, out total);
                return serList;
            }
        }
        private List<OBJTYPE> GetMainTableRecords(BigfootSQL.SqlHelper DB, int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, out int total, List<JoinData> Join = null) {
            if (Join != null) throw new InternalError("Join not supported");
            // get total # of records
            total = 0;
            DB.Clear();
            DB.SELECT("COUNT(*)").FROM(TableName);
            MakeFilter(DB, filters);

            total = DB.ExecuteScalarInt();
            List<OBJTYPE> list = null;

            DB.Clear();
            DB.SELECT("*");
            AddCalculatedProperties(DB, typeof(OBJTYPE));
            DB.FROM(TableName);
            MakeFilter(DB, filters);

            if (sort == null || sort.Count() == 0)
                sort = new List<DataProviderSortInfo> { new DataProviderSortInfo { Field = Key1Name, Order = DataProviderSortInfo.SortDirection.Ascending } };
            DB.ORDERBY(null, sort, Offset: skip, Next: take);
            list = DB.ExecuteCollection<OBJTYPE>();
            ReadSubTableRecords(DB, list);// now read all subtables for the records we have
            return list;
        }
        private void ReadSubTableRecords(BigfootSQL.SqlHelper DB, List<OBJTYPE> list) {
            foreach (var obj in list) {
                DB.Clear();
                ReadSubTables(DB, TableName, IdentityName, GetPropertyData(), obj, typeof(OBJTYPE));
            }
        }

        public OBJTYPE GetOneRecord(List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            //filters = NormalizeFilter(typeof(OBJTYPE), filters);
            throw new InternalError("Not implemented");
        }

        public bool IsInstalled() {
            bool status = false;
            using (SqlConnection conn = new SqlConnection(ConnString)) {
                Database db = GetDatabase(conn);
                status = db.Tables.Contains(TableName);
            }
            return status;
        }

        public bool InstallModel(List<string> errorList) {
            bool success = false;
            using (SqlConnection conn = new SqlConnection(ConnString)) {
                Database db = GetDatabase(conn);
                success = CreateTableWithBaseType(conn, db, errorList);
            }
            return success;
        }
        private bool CreateTableWithBaseType(SqlConnection conn, Database db, List<string> errorList) {
            Type baseType = typeof(BASEOBJTYPE);
            List<string> columns = new List<string>();
            if (!CreateTable(conn, db, BaseTableName, Key1Name, null, IdentityName, GetBasePropertyData(), baseType, errorList, columns,
                    TopMost: true,
                    SiteSpecific: CurrentSiteIdentity > 0,
                    DerivedDataTableName: "DerivedDataTableName", DerivedDataTypeName: "DerivedDataType", DerivedAssemblyName: "DerivedAssemblyName",
                    UseIdentity: false))
                return false;
            return CreateTable(conn, db, TableName, Key1Name, null, IdentityName, GetPropertyData(), typeof(OBJTYPE), errorList, columns,
                TopMost: true,
                SiteSpecific: CurrentSiteIdentity > 0,
                ForeignKeyTable: BaseTableName, UseIdentity: UseIdentity);
        }
        public bool UninstallModel(List<string> errorList) {
            try {
                using (SqlConnection conn = new SqlConnection(ConnString)) {
                    Database db = GetDatabase(conn);
                    DropTableWithBaseType(conn, db, errorList);
                }
                return true;
            } catch (Exception exc) {
                errorList.Add(string.Format("{0}: {1}", typeof(OBJTYPE).FullName, exc.Message));
                return false;
            }
        }
        public bool DropTableWithBaseType(SqlConnection conn, Database db, List<string> errorList) {
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                try {
                    if (db.Tables.Contains(TableName)) {
                        // Remove all records from the table (this removes the records in BaseTableName also)
                        BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);
                        DB.DELETE(BaseTableName).FROM(BaseTableName).INNERJOIN(TableName)
                            .ON(BaseTableName + "." + Key1Name, TableName + "." + Key1Name).ExecuteNonquery();
                        // then drop the table
                        db.Tables[TableName].Drop();
                    }
                    if (db.Tables.Contains(BaseTableName)) {
                        BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);
                        int count = DB.SELECT("COUNT(*)").FROM(BaseTableName).ExecuteScalarInt();
                        if (count == 0)
                            db.Tables[BaseTableName].Drop();
                    }
                    //TODO: not currently supported  DropSubTables(conn, db, TableName, errorList);
                } catch (Exception exc) {
                    if (Logging) YetaWF.Core.Log.Logging.AddErrorLog("Couldn't drop table", exc);
                    errorList.Add(string.Format("Couldn't drop table - {0}.", exc.Message));
                    return false;
                }
                return true;
            }
        }
        public void AddSiteData() { }
        public void RemoveSiteData() { // remove site-specific data
            if (CurrentSiteIdentity > 0) {
                using (SqlConnection connection = new SqlConnection(ConnString)) {
                    connection.Open();
                    BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);
                    DB.DELETEFROM(TableName).WHERE(SiteColumn, CurrentSiteIdentity).ExecuteScalar();
                    DB.Clear();
                    DB.DELETEFROM(BaseTableName).WHERE(SiteColumn, CurrentSiteIdentity).AND("DerivedDataTableName", TableName).ExecuteScalar();
                }
            }
        }
        public bool ExportChunk(int chunk, SerializableList<SerializableFile> fileList, out object obj) {
            using (SqlConnection connection = new SqlConnection(ConnString)) {
                connection.Open();
                BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, Languages);
                DB.SELECT("*")
                    .FROM(TableName)
                    .INNERJOIN(BaseTableName).ON(BaseTableName + "." + Key1Name, TableName + "." + Key1Name);
                if (CurrentSiteIdentity > 0)
                    DB.ANDON(BaseTableName + "." + SiteColumn, TableName + "." + SiteColumn);
                if (CurrentSiteIdentity > 0)
                    DB.WHERE(TableName + "." + SiteColumn, CurrentSiteIdentity);
                DB.ORDERBY(TableName + "." + Key1Name, Offset: chunk * ChunkSize, Next: ChunkSize);
                List<OBJTYPE> list = DB.ExecuteCollection<OBJTYPE>();
                //ReadSubTableRecords(DB, list);// now read all subtables for the records we have
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
                    Type baseType = typeof(BASEOBJTYPE);
                    using (SqlConnection connection = new SqlConnection(ConnString)) {
                        connection.Open();
                        for (int processed = 0 ; processed < total ; ++processed) {
                            using (SqlTransaction tr = connection.BeginTransaction()) {
                                subDBs = new List<BigfootSQL.SqlHelper>();
                                BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(connection, tr, Languages);
                                OBJTYPE item = serList[processed];
                                DB.INSERTINTO(BaseTableName, GetColumnList(DB, GetBasePropertyData(), baseType, "", true, WithDerivedInfo: true, SiteSpecific: CurrentSiteIdentity > 0))
                                    .VALUES(GetValueList(DB, BaseTableName, GetBasePropertyData(), item, baseType, DerivedType: typeof(OBJTYPE), DerivedTableName: TableName, SiteSpecific: CurrentSiteIdentity > 0));
                                if (subDBs.Count > 0)
                                    throw new InternalError("Subtable not supported for table {0}", TableName);
                                int identity = 0;
                                if (UseIdentity) {
                                    identity = DB.ExecuteScalarIdentity();
                                    if (identity == 0) throw new InternalError("Insert failed for type {0}", baseType.FullName);
                                } else
                                    DB.ExecuteNonquery();
                                DB.Clear();
                                DB.INSERTINTO(TableName, GetColumnList(DB, GetPropertyData(), item.GetType(), "", true, SiteSpecific: CurrentSiteIdentity > 0))
                                    .VALUES(GetValueList(DB, TableName, GetPropertyData(), item, typeof(OBJTYPE), SiteSpecific: CurrentSiteIdentity > 0))
                                    .ExecuteNonquery();
                                if (subDBs.Count > 0)
                                    throw new InternalError("Subtable not supported for table {0}", TableName);
                                tr.Commit();
                            }
                        }
                    }
                }
            }
        }
    }
}