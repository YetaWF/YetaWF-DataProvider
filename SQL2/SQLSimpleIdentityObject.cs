/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Models;
using YetaWF.Core.Support;
#if MVC6
using Microsoft.Data.SqlClient;
#else
using System.Data.SqlClient;
#endif

namespace YetaWF.DataProvider.SQL2 {

    /// <summary>
    /// This class implements access to objects (records), with a primary and secondary key (composite) and with an identity column.
    /// </summary>
    public partial class SQLSimple2IdentityObject<KEYTYPE, KEYTYPE2, OBJTYPE> : SQLSimpleIdentityObjectBase<KEYTYPE, KEYTYPE2, OBJTYPE> {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="options">A dictionary of options and optional parameters as provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.</param>
        /// <remarks>
        /// Data providers are instantiated when the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method is called, usually by an application data provider.
        /// </remarks>
        public SQLSimple2IdentityObject(Dictionary<string, object> options) : base(options, HasKey2: true) { }

    }
    /// <summary>
    /// This class implements access to objects (records), with one primary key and with an identity column.
    /// </summary>
    public partial class SQLSimpleIdentityObject<KEYTYPE, OBJTYPE> : SQLSimpleIdentityObjectBase<KEYTYPE, object, OBJTYPE> {

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="options">A dictionary of options and optional parameters as provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.</param>
        /// <remarks>
        /// Data providers are instantiated when the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method is called, usually by an application data provider.
        /// </remarks>
        public SQLSimpleIdentityObject(Dictionary<string, object> options) : base(options) { }
    }

    /// <summary>
    /// This base class implements access to objects, with a primary and secondary key (composite) and with an identity column.
    /// This base class is not intended for use by application data providers. These use one of the more specialized derived classes instead.
    /// </summary>
    public partial class SQLSimpleIdentityObjectBase<KEYTYPE, KEYTYPE2, OBJTYPE> : SQLSimpleObjectBase<KEYTYPE, KEYTYPE2, OBJTYPE>, IDataProviderIdentity<KEYTYPE, KEYTYPE2, OBJTYPE> {

        internal SQLSimpleIdentityObjectBase(Dictionary<string, object> options, bool HasKey2 = false) : base(options, HasKey2) { }

        /// <summary>
        /// Retrieves one record from the database table that satisfies the specified identity <paramref name="identity"/>.
        /// </summary>
        /// <param name="identity">The identity value.</param>
        /// <returns>Returns the record that satisfies the specified identity value. If no record exists null is returned.</returns>
        public async Task<OBJTYPE> GetByIdentityAsync(int identity) {

            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            sqlHelper.AddParam("ValIdentity", identity);

            using (SqlDataReader reader = await sqlHelper.ExecuteReaderStoredProcAsync($@"[{Dbo}].[{Dataset}__GetByIdentity]")) {
                if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) return default(OBJTYPE);
                OBJTYPE obj = sqlHelper.CreateObject<OBJTYPE>(reader);
                await ReadSubTablesAsync(sqlHelper, reader, Dataset, obj, typeof(OBJTYPE));
                return obj;
            }
        }

        /// <summary>
        /// Removes an existing record with the specified identity value.
        /// </summary>
        /// <param name="identity">The identity value of the record to remove.</param>
        /// <returns>Returns true if the record was removed, or false if the record was not found. Other errors cause an exception.</returns>
        public async Task<bool> RemoveByIdentityAsync(int identity) {
            SQLBuilder sb = new SQLBuilder();
            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = sb.GetTable(Database, Dbo, Dataset);
            List<PropertyData> propData = GetPropertyData();

            string subTablesDeletes = SubTablesDeletes(Dataset, propData, typeof(OBJTYPE));

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

            object val;
            try {
                val = await sqlHelper.ExecuteScalarAsync(script);
            } catch (Exception exc) {
                SqlException sqlExc = exc as SqlException;
                if (sqlExc != null && sqlExc.Number == 547) // ref integrity
                    return false;
                throw new InternalError("Delete failed for type {0} - {1}", typeof(OBJTYPE).FullName, ErrorHandling.FormatExceptionMessage(exc));
            }
            int deleted = Convert.ToInt32(val);
            if (deleted > 1)
                throw new InternalError($"More than 1 record deleted by {nameof(RemoveByIdentityAsync)} method");
            return deleted > 0;
        }

        /// <summary>
        /// Updates an existing record with the specified existing identity value <paramref name="identity"/> in the database table.
        /// The primary/secondary keys can be changed in the object.
        /// </summary>
        /// <param name="identity">The identity value of the record.</param>
        /// <param name="obj">The object being updated.</param>
        /// <returns>Returns a status indicator.</returns>
        public async Task<UpdateStatusEnum> UpdateByIdentityAsync(int identity, OBJTYPE obj) {
            SQLBuilder sb = new SQLBuilder();
            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = sb.GetTable(Database, Dbo, Dataset);
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
                SqlException sqlExc = exc as SqlException;
                if (sqlExc != null && sqlExc.Number == 2627) {
                    // duplicate key violation, meaning the new key already exists
                    return UpdateStatusEnum.NewKeyExists;
                }
                throw new InternalError($"Update failed for type {typeof(OBJTYPE).FullName} - {ErrorHandling.FormatExceptionMessage(exc)}");
            }
            return UpdateStatusEnum.OK;
        }
    }
}
