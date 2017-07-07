/* Copyright © 2017 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider
{
    public partial class SQLIdentityObjectDataProvider<KEYTYPE, KEY2TYPE, IDENTITYTYPE, OBJTYPE> :
        SQLSimpleObjectDataProvider<KEYTYPE, OBJTYPE>,
        IDataProviderIdentity<KEYTYPE, KEY2TYPE, IDENTITYTYPE, OBJTYPE>
    {
        private const int ChunkSize = 100;

        public SQLIdentityObjectDataProvider(string table, string dbOwner, string connString, int dummy = 0,
                int CurrentSiteIdentity = 0,
                bool NoLanguages = false,
                bool Cacheable = false,
                bool Logging = true,
                int IdentitySeed = 0,
                Func<string, string> CalculatedPropertyCallback = null)
            : base(table, dbOwner, connString, 0, CurrentSiteIdentity, NoLanguages, Cacheable, Logging, IdentitySeed, CalculatedPropertyCallback) { }

        public string Key2Name { get { return GetKey2Name(TableName, ObjectSupport.GetPropertyData(typeof(OBJTYPE))); } }

        public OBJTYPE Get(KEYTYPE key, KEY2TYPE key2) {
            BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(Conn, Languages);

            DB.SELECT("TOP 1 *");
            AddCalculatedProperties(DB, typeof(OBJTYPE));
            DB.FROM(TableName);
            DB.Add("WITH(NOLOCK)");
            MakeJoins(DB, null);
            DB.WHERE(Key1Name, key);
            if (typeof(KEY2TYPE) != typeof(object))
                DB.AND(Key2Name, key2);
            if (CurrentSiteIdentity > 0)
                DB.AND(SiteColumn, CurrentSiteIdentity);
            OBJTYPE obj = DB.ExecuteObject<OBJTYPE>();
            if (obj != null)
                ReadSubTables(DB, TableName, IdentityName, ObjectSupport.GetPropertyData(typeof(OBJTYPE)), obj, typeof(OBJTYPE));
            return obj;
        }
        public OBJTYPE GetByIdentity(IDENTITYTYPE id) {
            BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(Conn, Languages);

            DB.SELECT("TOP 1 *");
            AddCalculatedProperties(DB, typeof(OBJTYPE));
            DB.FROM(TableName);
            DB.Add("WITH(NOLOCK)");
            MakeJoins(DB, null);
            DB.WHERE(IdentityName, id);

            OBJTYPE obj = DB.ExecuteObject<OBJTYPE>();
            if (obj != null)
                ReadSubTables(DB, TableName, IdentityName, ObjectSupport.GetPropertyData(typeof(OBJTYPE)), obj, typeof(OBJTYPE));
            return obj;
        }
        public UpdateStatusEnum Update(KEYTYPE origKey, KEY2TYPE origKey2, KEYTYPE newKey, KEY2TYPE newKey2, OBJTYPE obj) {
            BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(Conn, Languages);
            DB.UPDATE(TableName);
            AddSetColumns(DB, TableName, IdentityName, ObjectSupport.GetPropertyData(typeof(OBJTYPE)), obj, typeof(OBJTYPE));
            DB.WHERE(Key1Name, origKey);
            if (typeof(KEY2TYPE) != typeof(object))
                DB.AND(Key2Name, origKey2);
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
                if (!newKey.Equals(origKey) || !newKey2.Equals(origKey2)) {
                    SqlException sqlExc = exc as SqlException;
                    if (sqlExc != null && sqlExc.Number == 2627) {
                        // duplicate key violation, meaning the new key already exists
                        return UpdateStatusEnum.NewKeyExists;
                    }
                }
                throw new InternalError("Update failed for type {0} - {1}", typeof(OBJTYPE).FullName, exc.Message);
            }
            return UpdateStatusEnum.OK;
        }
        public UpdateStatusEnum UpdateByIdentity(IDENTITYTYPE id, OBJTYPE obj) {
            BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(Conn, Languages);
            DB.UPDATE(TableName);
            AddSetColumns(DB, TableName, IdentityName, ObjectSupport.GetPropertyData(typeof(OBJTYPE)), obj, typeof(OBJTYPE));
            DB.WHERE(IdentityName, id);
            DB.SELECT("@@ROWCOUNT");
            try {
                int changed = DB.ExecuteScalarInt();
                if (changed == 0)
                    return UpdateStatusEnum.RecordDeleted;
                if (changed > 1)
                    throw new InternalError("Update failed - {0} records updated", changed);
            } catch (Exception exc) {
                SqlException sqlExc = exc as SqlException;
                if (sqlExc != null && sqlExc.Number == 2627) {
                    // duplicate key violation, meaning the new key already exists
                    return UpdateStatusEnum.NewKeyExists;
                }
                throw new InternalError("Update failed for type {0} - {1}", typeof(OBJTYPE).FullName, exc.Message);
            }
            return UpdateStatusEnum.OK;
        }
        public new bool Add(OBJTYPE obj) {
            IDataProvider<KEYTYPE, OBJTYPE> dp = (IDataProvider<KEYTYPE, OBJTYPE>) this;
            return dp.Add(obj);
        }
        public bool Remove(KEYTYPE key, KEY2TYPE key2) {
            BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(Conn, Languages);
            DB.DELETEFROM(TableName);
            DB.WHERE(Key1Name, key);
            if (typeof(KEY2TYPE) != typeof(object))
                DB.AND(Key2Name, key2);
            if (CurrentSiteIdentity > 0)
                DB.AND(SiteColumn, CurrentSiteIdentity);
            DB.SELECT("@@ROWCOUNT");
            int deleted = DB.ExecuteScalarInt();
            if (deleted > 1) throw new InternalError("More than 1 record deleted by Remove() method");
            return deleted > 0;
        }
        public bool RemoveByIdentity(IDENTITYTYPE id) {
            BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(Conn, Languages);
            DB.DELETEFROM(TableName);
            DB.WHERE(IdentityName, id);
            DB.SELECT("@@ROWCOUNT");
            int deleted = DB.ExecuteScalarInt();
            if (deleted > 1) throw new InternalError("More than 1 record deleted by Remove() method");
            return deleted > 0;
        }
        public new int RemoveRecords(List<DataProviderFilterInfo> filters) {
            IDataProvider<KEYTYPE, OBJTYPE> dp = (IDataProvider<KEYTYPE, OBJTYPE>) this;
            return dp.RemoveRecords(filters);
        }
        public new List<OBJTYPE> GetRecords(int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, out int total, List<JoinData> Joins = null) {
            IDataProvider<KEYTYPE, OBJTYPE> dp = (IDataProvider<KEYTYPE, OBJTYPE>) this;
            return dp.GetRecords(skip, take, sort, filters, out total, Joins: Joins);
        }
        public new OBJTYPE GetOneRecord(List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            IDataProvider<KEYTYPE, OBJTYPE> dp = (IDataProvider<KEYTYPE, OBJTYPE>) this;
            return dp.GetOneRecord(filters, Joins);
        }
        public new bool IsInstalled() {
            IDataProvider<KEYTYPE, OBJTYPE> dp = (IDataProvider<KEYTYPE, OBJTYPE>) this;
            return dp.IsInstalled();
        }
        public new bool InstallModel(List<string> errorList) {
            bool success = false;
            Database db = GetDatabase();
            List<string> columns = new List<string>();
            success = CreateTable(db, TableName, Key1Name, Key2Name, IdentityName, ObjectSupport.GetPropertyData(typeof(OBJTYPE)), typeof(OBJTYPE), errorList, columns,
                SiteSpecific: CurrentSiteIdentity > 0,
                TopMost: true, UseIdentity: UseIdentity);
            SqlCache.ClearCache();
            return success;
        }
        public new bool UninstallModel(List<string> errorList) {
            IDataProvider<KEYTYPE, OBJTYPE> dp = (IDataProvider<KEYTYPE, OBJTYPE>) this;
            return dp.UninstallModel(errorList);
        }
        public new void AddSiteData() {
            IDataProvider<KEYTYPE, OBJTYPE> dp = (IDataProvider<KEYTYPE, OBJTYPE>) this;
            dp.AddSiteData();
        }
        public new void RemoveSiteData() {
            IDataProvider<KEYTYPE, OBJTYPE> dp = (IDataProvider<KEYTYPE, OBJTYPE>) this;
            dp.RemoveSiteData();
        }
        public new bool ExportChunk(int chunk, SerializableList<SerializableFile> fileList, out object obj) {
            BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(Conn, Languages);
            DB.SELECT("*").FROM(TableName);
            if (CurrentSiteIdentity > 0)
                DB.WHERE(SiteColumn, CurrentSiteIdentity);
            if (Key2Name != null)
                DB.ORDERBY(SQLDataProviderImpl.WrapBrackets(Key1Name) + "," + SQLDataProviderImpl.WrapBrackets(Key2Name), Offset: chunk * ChunkSize, Next: ChunkSize);
            else
                DB.ORDERBY(SQLDataProviderImpl.WrapBrackets(Key1Name), Offset: chunk * ChunkSize, Next: ChunkSize);
            List<OBJTYPE> list = DB.ExecuteCollection<OBJTYPE>();
            SerializableList<OBJTYPE> serList = new SerializableList<OBJTYPE>(list);
            obj = serList;
            int count = serList.Count();
            if (count == 0)
                obj = null;
            return (count >= ChunkSize);
        }
        public new void ImportChunk(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            if (CurrentSiteIdentity > 0 || YetaWFManager.Manager.ImportChunksNonSiteSpecifics) {
                IDataProvider<KEYTYPE, OBJTYPE> dp = (IDataProvider<KEYTYPE, OBJTYPE>)this;
                dp.ImportChunk(chunk, fileList, obj);
            }
        }
    }
}
