/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.PostgreSQL {

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

            AddSubtableMapping();
            sqlHelper.AddParam("valIdentity", identity);

            try {
                using (NpgsqlDataReader reader = await sqlHelper.ExecuteReaderStoredProcAsync($@"""{Schema}"".""{Dataset}__GetByIdentity""")) {
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) return default(OBJTYPE);
                    return sqlHelper.CreateObject<OBJTYPE>(reader);
                }
            } catch (Exception exc) {
                Npgsql.PostgresException sqlExc = exc as Npgsql.PostgresException;
                if (sqlExc != null)
                    throw new InternalError($"{nameof(GetByIdentityAsync)} failed for type {typeof(OBJTYPE).FullName} - {sqlExc.Detail} - {ErrorHandling.FormatExceptionMessage(exc)}");
                throw new InternalError($"{nameof(GetByIdentityAsync)} failed for type {typeof(OBJTYPE).FullName} - {ErrorHandling.FormatExceptionMessage(exc)}");
            }
        }

        /// <summary>
        /// Updates an existing record with the specified existing identity value <paramref name="identity"/> in the database table.
        /// The primary/secondary keys can be changed in the object.
        /// </summary>
        /// <param name="identity">The identity value of the record.</param>
        /// <param name="obj">The object being updated.</param>
        /// <returns>Returns a status indicator.</returns>
        public async Task<UpdateStatusEnum> UpdateByIdentityAsync(int identity, OBJTYPE obj) {

            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            AddSubtableMapping();
            GetParameterList(sqlHelper, obj, Database, Schema, Dataset, GetPropertyData(), Prefix: null, TopMost: true, SiteSpecific: false, WithDerivedInfo: false, SubTable: false);
            sqlHelper.AddParam("valIdentity", identity);

            try {
                using (NpgsqlDataReader reader = await sqlHelper.ExecuteReaderStoredProcAsync($@"""{Schema}"".""{Dataset}__UpdateByIdentity""")) {
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync()))
                        throw new InternalError($"No result set received from {Dataset}__UpdateByIdentity");
                    int changed = Convert.ToInt32(reader[0]);
                    if (changed == 0)
                        return UpdateStatusEnum.RecordDeleted;
                    if (changed > 1)
                        throw new InternalError($"Update failed - {changed} records updated");
                }
            } catch (Exception exc) {
                Npgsql.PostgresException sqlExc = exc as Npgsql.PostgresException;
                if (sqlExc != null && sqlExc.SqlState == PostgresErrorCodes.UniqueViolation) // already exists
                    return UpdateStatusEnum.NewKeyExists;
                if (sqlExc != null)
                    throw new InternalError($"{nameof(UpdateByIdentityAsync)} failed for type {typeof(OBJTYPE).FullName} - {sqlExc.Detail} - {ErrorHandling.FormatExceptionMessage(exc)}");
                throw new InternalError($"{nameof(UpdateByIdentityAsync)} failed for type {typeof(OBJTYPE).FullName} - {ErrorHandling.FormatExceptionMessage(exc)}");
            }
            return UpdateStatusEnum.OK;
        }

        /// <summary>
        /// Removes an existing record with the specified identity value.
        /// </summary>
        /// <param name="identity">The identity value of the record to remove.</param>
        /// <returns>Returns true if the record was removed, or false if the record was not found. Other errors cause an exception.</returns>
        public async Task<bool> RemoveByIdentityAsync(int identity) {

            await EnsureOpenAsync();

            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            sqlHelper.AddParam("valIdentity", identity);

            try {
                using (NpgsqlDataReader reader = await sqlHelper.ExecuteReaderStoredProcAsync($@"""{Schema}"".""{Dataset}__RemoveByIdentity""")) {
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync()))
                        throw new InternalError($"No result set received from {Dataset}__RemoveByIdentity");
                    int deleted = Convert.ToInt32(reader[0]);
                    if (deleted > 1)
                        throw new InternalError($"More than 1 record deleted by {nameof(RemoveByIdentityAsync)} method");
                    return deleted > 0;
                }
            } catch (Exception exc) {
                Npgsql.PostgresException sqlExc = exc as Npgsql.PostgresException;
                if (sqlExc != null && sqlExc.SqlState == PostgresErrorCodes.UniqueViolation) //$$$$$ ref integrity
                    return false;
                if (sqlExc != null)
                    throw new InternalError($"{nameof(RemoveByIdentityAsync)} failed for type {typeof(OBJTYPE).FullName} - {sqlExc.Detail} - {ErrorHandling.FormatExceptionMessage(exc)}");
                throw new InternalError($"{nameof(RemoveByIdentityAsync)} failed for type {typeof(OBJTYPE).FullName} - {ErrorHandling.FormatExceptionMessage(exc)}");
            }
        }
    }
}
