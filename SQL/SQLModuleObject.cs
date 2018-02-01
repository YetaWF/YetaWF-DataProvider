using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
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

        public new OBJTYPE Get(KEY key) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullBaseTableName = SQLBuilder.GetTable(Database, Dbo, BaseDataset);

            if (Dataset == BaseDataset) {
                // we're reading the base and have to find the derived table

                string scriptMain = $@"
SELECT TOP 1 *
INTO #BASETABLE
FROM {fullBaseTableName} WITH(NOLOCK)
WHERE {sqlHelper.Expr(Key1Name, "=", key)} {AndSiteIdentity}

IF @@ROWCOUNT > 0 
BEGIN

DECLARE @Table nvarchar(80);
DECLARE @Type nvarchar(200);
DECLARE @Asm nvarchar(200);

SELECT @Table=[DerivedDataTableName], @Type=[DerivedDataType], @Asm=[DerivedAssemblyName] FROM #BASETABLE
;
SELECT @Table, @Type, @Asm FROM #BASETABLE --- result set
; 
EXEC ('SELECT TOP 1 * FROM #BASETABLE A, [' + @Table + '] B WHERE B.{Key1Name} = ''{key}''') --- result set
END

DROP TABLE #BASETABLE

{sqlHelper.DebugInfo}";

                using (SqlDataReader reader = sqlHelper.ExecuteReader(scriptMain)) {
                    if (!reader.Read())
                        return default(OBJTYPE);
                    string derivedTableName = (string)reader[0];
                    string derivedDataType = (string)reader[1];
                    string derivedAssemblyName = (string)reader[2];
                    if (string.IsNullOrWhiteSpace(derivedTableName))
                        return default(OBJTYPE);
                    if (!reader.NextResult())
                        return default(OBJTYPE);
                    if (!reader.Read()) return default(OBJTYPE);
                    OBJTYPE obj = sqlHelper.CreateObject<OBJTYPE>(reader, derivedDataType, derivedAssemblyName);
                    return obj;
                }

            } else {
                // we're reading the derived table
                string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
                string scriptMain = $@"
SELECT TOP 1 * FROM {fullTableName} WITH(NOLOCK)
INNER JOIN {fullBaseTableName} ON 
    {fullBaseTableName}.[{Key1Name}] = {fullTableName}.[{Key1Name}] AND {fullBaseTableName}.[{SiteColumn}] = {fullTableName}.[{SiteColumn}]
WHERE {sqlHelper.Expr(Key1Name, "=", key)} {AndSiteIdentity}

{sqlHelper.DebugInfo}";

                using (SqlDataReader reader = sqlHelper.ExecuteReader(scriptMain)) {
                    if (!reader.Read()) return default(OBJTYPE);
                    OBJTYPE obj = sqlHelper.CreateObject<OBJTYPE>(reader);
                    return obj;
                }
            }
        }

        public new bool Add(OBJTYPE obj) {
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
                sqlHelper.ExecuteNonQuery(scriptMain);
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

        public new UpdateStatusEnum Update(KEY origKey, KEY newKey, OBJTYPE obj) {
            if (Dataset == BaseDataset) throw new InternalError("Only derived types are supported");

            if (!origKey.Equals(newKey)) throw new InternalError("Can't change key");

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullBaseTableName = SQLBuilder.GetTable(Database, Dbo, BaseDataset);
            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);

            List<PropertyData> propBaseData = GetBasePropertyData();
            string setBaseColumns = SetColumns(sqlHelper, Dataset, IdentityName, propBaseData, obj, typeof(ModuleDefinition));
            List<PropertyData> propData = GetPropertyData();
            string setColumns = SetColumns(sqlHelper, Dataset, IdentityName, propData, obj, typeof(OBJTYPE));

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
                object val = sqlHelper.ExecuteScalar(scriptMain);
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

        public new bool Remove(KEY key) {
            if (Dataset != BaseDataset) throw new InternalError("Only base types are supported");

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullBaseTableName = SQLBuilder.GetTable(Database, Dbo, BaseDataset);

            List<PropertyData> propData = GetPropertyData();

            string scriptMain = $@"
SELECT TOP 1 *
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
            object val = sqlHelper.ExecuteScalar(scriptMain);
            int deleted = Convert.ToInt32(val);
            if (deleted > 1)
                throw new InternalError($"More than 1 record deleted by {nameof(Remove)} method");
            return deleted > 0;
        }

        public new OBJTYPE GetOneRecord(List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            throw new NotImplementedException();
        }

        public new List<OBJTYPE> GetRecords(int skip, int take, List<DataProviderSortInfo> sorts, List<DataProviderFilterInfo> filters, out int total, List<JoinData> Joins = null) {
            if (Dataset != BaseDataset) throw new InternalError("Only base dataset is supported");
            return base.GetRecords(skip, take, sorts, filters, out total, Joins: Joins);
        }

        public new int RemoveRecords(List<DataProviderFilterInfo> filters) {
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

        public new bool IsInstalled() {
            if (!SQLCache.HasTable(Conn, Database, BaseDataset))
                return false;
            if (Dataset != BaseDataset) { 
                if (!SQLCache.HasTable(Conn, Database, Dataset))
                    return false;
            }
            return true;
        }

        public new bool InstallModel(List<string> errorList) {
            if (Dataset == BaseDataset) throw new InternalError("Base dataset is not supported");
            bool success = false;
            Database db = GetDatabase();
            success = CreateTableWithBaseType(db, errorList);
            SQLCache.ClearCache();
            return success;
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

        public new bool UninstallModel(List<string> errorList) {
            if (Dataset == BaseDataset) throw new InternalError("Base dataset is not supported");
            try {
                Database db = GetDatabase();
                DropTableWithBaseType(db, errorList);
                return true;
            } catch (Exception exc) {
                errorList.Add(string.Format("{0}: {1}", typeof(OBJTYPE).FullName, exc.Message));
                return false;
            } finally {
                SQLCache.ClearCache();
            }
        }
        private bool DropTableWithBaseType(Database db, List<string> errorList) {
            try {
                if (db.Tables.Contains(Dataset)) {
                    // Remove all records from the table (this removes the records in BaseTableName also)
                    SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                    SQLBuilder sb = new SQL.SQLBuilder();
                    sb.Add($@"
DELETE {BaseDataset} FROM {BaseDataset}
    INNER JOIN {Dataset} ON {BaseDataset}.[{Key1Name}] = {Dataset}.[{Key1Name}]
                    ");
                    sqlHelper.ExecuteNonQuery(sb.ToString());
                    // then drop the table
                    db.Tables[Dataset].Drop();
                }
                if (db.Tables.Contains(BaseDataset)) {
                    SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                    SQLBuilder sb = new SQL.SQLBuilder();
                    sb.Add($@"
SELECT COUNT(*) FROM  {BaseDataset}
");
                    object val = sqlHelper.ExecuteScalar(sb.ToString());
                    int count = Convert.ToInt32(val);
                    if (count == 0)
                        db.Tables[BaseDataset].Drop();
                }
            } catch (Exception exc) {
                if (Logging) YetaWF.Core.Log.Logging.AddErrorLog("Couldn't drop table", exc);
                errorList.Add(string.Format("Couldn't drop table - {0}.", exc.Message));
                return false;
            }
            return true;
        }
        public new void RemoveSiteData() { // remove site-specific data
            if (Dataset == BaseDataset) throw new InternalError("Base dataset is not supported");
            if (SiteIdentity > 0) {
                SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                SQLBuilder sb = new SQL.SQLBuilder();
                sb.Add($@"
DELETE FROM {Dataset} WHERE [{SiteColumn}] = {SiteIdentity}
DELETE FROM {BaseDataset} WHERE [DerivedDataTableName] = '{Dataset}' AND [{SiteColumn}] = {SiteIdentity}
");
                sqlHelper.ExecuteNonQuery(sb.ToString());
            }
        }

        public new void ImportChunk(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            if (Dataset == BaseDataset) throw new InternalError("Base dataset is not supported");
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

        public new bool ExportChunk(int chunk, SerializableList<SerializableFile> fileList, out object obj) {
            if (Dataset == BaseDataset) throw new InternalError("Base dataset is not supported");

            List<DataProviderSortInfo> sorts = new List<DataProviderSortInfo> { new DataProviderSortInfo { Field = Key1Name, Order = DataProviderSortInfo.SortDirection.Ascending } };
            int total;
            List<OBJTYPE> list = GetRecords(chunk * ChunkSize, ChunkSize, sorts, null, out total);
            obj = new SerializableList<OBJTYPE>(list);

            int count = list.Count();
            if (count == 0)
                obj = null;
            return (count >= ChunkSize);
        }
        public bool ExportChunk(int chunk, SerializableList<SerializableFile> fileList, Type type, out object obj) {
            throw new InternalError("Typed ExportChunk not supported");
        }
    }
}
