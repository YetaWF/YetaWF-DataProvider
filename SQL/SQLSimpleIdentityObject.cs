/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Models;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.SQL {

    public partial class SQLSimple2IdentityObject<KEYTYPE, KEYTYPE2, OBJTYPE> : SQLSimpleIdentityObjectBase<KEYTYPE, KEYTYPE2, OBJTYPE> {
        public SQLSimple2IdentityObject(Dictionary<string, object> options) : base(options, HasKey2: true) { }
    }
    public partial class SQLSimpleIdentityObject<KEYTYPE, OBJTYPE> : SQLSimpleIdentityObjectBase<KEYTYPE, object, OBJTYPE> {
        public SQLSimpleIdentityObject(Dictionary<string, object> options) : base(options) { }
    }
    public partial class SQLSimpleIdentityObjectBase<KEYTYPE, KEYTYPE2, OBJTYPE> : SQLSimpleObjectBase<KEYTYPE, KEYTYPE2, OBJTYPE>, IDataProviderIdentity<KEYTYPE, KEYTYPE2, OBJTYPE> {
    
        public SQLSimpleIdentityObjectBase(Dictionary<string, object> options, bool HasKey2 = false) : base(options) {
            this.HasKey2 = HasKey2;
        }

        public async Task<OBJTYPE> GetByIdentityAsync(int identity) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string joins = null;// RFFU
            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            string calcProps = await CalculatedPropertiesAsync(typeof(OBJTYPE));

            List<PropertyData> propData = GetPropertyData();
            string subTablesSelects = SubTablesSelects(Dataset, propData, typeof(OBJTYPE));

            string script = $@"
SELECT TOP 1 *
    {calcProps} 
FROM {fullTableName} WITH(NOLOCK) {joins}
WHERE {sqlHelper.Expr(IdentityName, "=", identity)} {AndSiteIdentity}  -- result set
;

{subTablesSelects}

{sqlHelper.DebugInfo}";

            using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(script)) {
                if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) return default(OBJTYPE);
                OBJTYPE obj = sqlHelper.CreateObject<OBJTYPE>(reader);
                if (!string.IsNullOrWhiteSpace(subTablesSelects)) {
                    await ReadSubTablesAsync(sqlHelper, reader, Dataset, obj, propData, typeof(OBJTYPE));
                }
                return obj;
            }
        }

        public async Task<bool> RemoveByIdentityAsync(int identity) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = GetPropertyData();

            string subTablesDeletes = SubTablesDeletes(fullTableName, propData, typeof(OBJTYPE));

            string scriptMain = $@"
DELETE
FROM {fullTableName} 
WHERE {sqlHelper.Expr(IdentityName, "=", identity)} {AndSiteIdentity}
;
SELECT @@ROWCOUNT --- result set

{sqlHelper.DebugInfo}";

            string scriptWithSub = $@"
DECLARE @ident int = {identity};

DELETE
FROM {fullTableName} 
WHERE [{IdentityName}] = @ident
;
SELECT @@ROWCOUNT --- result set

{subTablesDeletes}

{sqlHelper.DebugInfo}";

            string script = (string.IsNullOrWhiteSpace(subTablesDeletes)) ? scriptMain : scriptWithSub;

            object val = await sqlHelper.ExecuteScalarAsync(script);
            int deleted = Convert.ToInt32(val);
            if (deleted > 1)
                throw new InternalError($"More than 1 record deleted by {nameof(RemoveByIdentityAsync)} method");
            return deleted > 0;
        }

        public async Task<UpdateStatusEnum> UpdateByIdentityAsync(int identity, OBJTYPE obj) {

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = SQLBuilder.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = GetPropertyData();
            string setColumns = SetColumns(sqlHelper, Dataset, propData, obj, typeof(OBJTYPE));

            string subTablesUpdates = SubTablesUpdates(sqlHelper, Dataset, obj, propData, typeof(OBJTYPE));

            string scriptMain = $@"
UPDATE {fullTableName} 
SET {setColumns}
WHERE {sqlHelper.Expr(IdentityName, "=", identity)} {AndSiteIdentity}
;
SELECT @@ROWCOUNT --- result set

{sqlHelper.DebugInfo}";

            string scriptWithSub = $@"
DECLARE @__IDENTITY int = {identity};

UPDATE {fullTableName} 
SET {setColumns}
WHERE [{IdentityName}] = @__IDENTITY
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
                throw new InternalError($"Update failed for type {typeof(OBJTYPE).FullName} - {exc.Message}");
            }
            return UpdateStatusEnum.OK;
        }
    }
}
