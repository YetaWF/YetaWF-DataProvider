/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading.Tasks;
using System.Transactions;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Support;
using YetaWF.Core.Support.Serializers;
using YetaWF.DataProvider.SQLGeneric;
using YetaWF.Core.Language;
#if MVC6
using Microsoft.Data.SqlClient;
#else
using System.Data.SqlClient;
#endif

namespace YetaWF.DataProvider.SQL {

    /// <summary>
    /// This abstract class is the base class for all SQL low-level data providers.
    /// </summary>
    public abstract class SQLBase : SQLGenericBase, IDataProviderTransactions {

        /// <summary>
        /// Defines the name of the SQL low-level data provider.
        /// This name is used in AppSettings.json ("IOMode": "SQL").
        /// </summary>
        public const string ExternalName = "SQL";

        /// <summary>
        /// Defines the key used in AppSettings.json to define a SQL connection string
        /// ("SQLConnect": "Data Source=...datasource...;Initial Catalog=...catalog...;User ID=..userid..;Password=..password..").
        /// </summary>
        public const string SQLConnectString = "SQLConnect";
        /// <summary>
        /// Defines the key used in AppSettings.json to define a SQL dbo.
        /// </summary>
        public const string SQLDboString = "SQLDbo";

        /// <summary>
        /// Defines the SQL connection string used by this data provider.
        /// </summary>
        /// <remarks>The connection string is defined in AppSettings.json but may be modified by the data provider.
        /// The ConnectionString property contains the actual connection string used to connect to the SQL database.
        /// </remarks>
        public string ConnectionString { get; private set; }
        /// <summary>
        /// Defines the database owner used by this data provider.
        /// </summary>
        /// <remarks>The database owner is defined in AppSettings.json ("SQLDbo": "dbo").
        /// </remarks>
        public string Dbo { get; private set; }
        /// <summary>
        /// The underlying Microsoft.Data.SqlClient.SqlConnection object used to connect to the database.
        /// </summary>
        public SqlConnection Conn { get; private set; }
        /// <summary>
        /// Dynamically allocated SQL connection with a use count, otherwise it's explicitly allocated.
        /// </summary>
        public bool ConnDynamic { get; private set; }

        internal string AndSiteIdentity { get; private set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="options">A dictionary of options and optional parameters as provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.</param>
        /// <param name="HasKey2">Defines whether the object has a secondary key.</param>
        /// <remarks>
        /// Data providers are instantiated when the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method is called, usually by an application data provider.
        ///
        /// For debugging purposes, instances of this class are tracked using the DisposableTracker class.
        /// </remarks>
        protected SQLBase(Dictionary<string, object> options, bool HasKey2 = false) : base(options, HasKey2) {
            SqlConnectionStringBuilder sqlsb = new SqlConnectionStringBuilder(GetSqlConnectionString());
            sqlsb.MultipleActiveResultSets = true;
            ConnectionString = sqlsb.ToString();
            Dbo = GetSqlDbo();

            if (SiteIdentity > 0)
                AndSiteIdentity = $"AND [{SiteColumn}] = {SiteIdentity}";

            Conn = GetSqlConnection(ConnectionString);
            ConnDynamic = true;
        }
        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="disposing">true to close the database connection and release the DisposableTracker reference count, false otherwise.</param>
        protected override void Dispose(bool disposing) {
            base.Dispose(disposing);
            if (disposing) {
                if (Conn != null) {
                    if (ConnDynamic)
                        ReleaseSqlConnection(ConnectionString);
                    else
                        Conn.Close();
                    Conn = null;
                }
            }
        }

        /// <summary>
        /// Called to make sure that the database connection has been opened.
        /// Any public API must call this as a data provider no longer opens the connection immediately.
        /// </summary>
        /// <remarks>Originally the connection was opened in the constructor (yeah, bad idea I had many years ago, before async).
        /// To avoid having to change the public APIs for data providers, this call in APIs offered by data providers opens the connection. All data provider APIs are async so no changes needed.</remarks>
        public Task EnsureOpenAsync() {
            lock (OpenLock) { // prevent concurrent Open calls, could cause errors when multiple threads try to open the same DB
                if (Conn.State == System.Data.ConnectionState.Closed) {
                    Conn.Open();
                }
            }
            Database = Conn.Database;
            return Task.CompletedTask;
        }
        private static readonly object OpenLock = new object();

        private class ConnectionEntry {
            public SqlConnection Conn { get; set; }
            public int UseCount { get; set; }
        }
        private SqlConnection GetSqlConnection(string connectionString) {
            lock (ConnectionCacheLock) {
                ConnectionEntry entry;
                if (ConnectionCache.TryGetValue(connectionString, out entry)) {
                    entry.UseCount++;
                    return entry.Conn;
                }
                entry = new ConnectionEntry {
                    Conn = new SqlConnection(connectionString),
                    UseCount = 1,
                };
                ConnectionCache.Add(connectionString, entry);
                return entry.Conn;
            }
        }
        private void ReleaseSqlConnection(string connectionString) {
            lock (ConnectionCacheLock) {
                ConnectionEntry entry;
                if (!ConnectionCache.TryGetValue(connectionString, out entry))
                    throw new InternalError($"Releasing unallocated sql connection");
                entry.UseCount--;
                if (entry.UseCount <= 0) {
                    ConnectionCache.Remove(connectionString);
                    entry.Conn.Close();
                    entry.Conn.Dispose();
                }
            }
        }

        private static Dictionary<string, ConnectionEntry> ConnectionCache = new Dictionary<string, ConnectionEntry>();
        private static object ConnectionCacheLock = new object();


        private string GetSqlConnectionString() {
            string connString = WebConfigHelper.GetValue<string>(string.IsNullOrWhiteSpace(WebConfigArea) ? Dataset : WebConfigArea, SQLConnectString);
            if (string.IsNullOrWhiteSpace(connString)) {
                if (string.IsNullOrWhiteSpace(WebConfigArea))
                    connString = WebConfigHelper.GetValue<string>(Package.AreaName, SQLConnectString);
                if (string.IsNullOrWhiteSpace(connString)) {
                    if (_defaultConnectString == null) {
                        _defaultConnectString = WebConfigHelper.GetValue<string>(DefaultString, SQLConnectString);
                        if (_defaultConnectString == null)
                            throw new InternalError("No SQLConnect connection string found (also no default)");
                    }
                    connString = _defaultConnectString;
                }
            }
            if (string.IsNullOrWhiteSpace(connString)) throw new InternalError($"No SQL connection string provided (also no default)");
            return connString;
        }
        private string GetSqlDbo() {
            string dbo = WebConfigHelper.GetValue<string>(string.IsNullOrWhiteSpace(WebConfigArea) ? Dataset : WebConfigArea, SQLDboString);
            if (string.IsNullOrWhiteSpace(dbo)) {
                if (string.IsNullOrWhiteSpace(WebConfigArea))
                    dbo = WebConfigHelper.GetValue<string>(Package.AreaName, SQLDboString);
                if (string.IsNullOrWhiteSpace(dbo)) {
                    if (_defaultDbo == null) {
                        _defaultDbo = WebConfigHelper.GetValue<string>(DefaultString, SQLDboString);
                        if (_defaultDbo == null)
                            throw new InternalError("No SQLDBo owner found (also no default)");
                    }
                    dbo = _defaultDbo;
                }
            }
            if (string.IsNullOrWhiteSpace(dbo)) throw new InternalError($"No SQL dbo provided (also no default)");
            return dbo;
        }
        private static string _defaultConnectString;
        private static string _defaultDbo;

        // IDATAPROVIDERTRANSACTIONS
        // IDATAPROVIDERTRANSACTIONS
        // IDATAPROVIDERTRANSACTIONS

        private TransactionScope Trans { get; set; }

        /// <summary>
        /// Starts a transaction that can be committed, saving all updates, or aborted to abandon all updates.
        /// </summary>
        /// <returns>Returns a YetaWF.Core.DataProvider.DataProviderTransaction object.</returns>
        /// <remarks>
        /// It is expected that the first dataprovider to be used will implicitly open the connection.
        /// Second, it is expected that all dataproviders will be disposed of around the same time(otherwise you'll get "can't access disposed object" for a connection.
        /// Lastly, if you use a dataprovider that is not the owner or listed as a dps parameter, you'll still get  'This platform does not support distributed transactions.'
        /// </remarks>
        public DataProviderTransaction StartTransaction(DataProviderImpl ownerDP, params DataProviderImpl[] dps) {
            // This is to work around the 'This platform does not support distributed transactions.'
            // problem because we are using multiple connections, even if it's the same database.
            // we consolidate all dataproviders to use one dataprovider (the owner).
            // In order to participate in the transaction we have to open the connection, so we use a non-cached,
            // non-dynamic connection (for all data providers).

            // release the owner's current connection
            SQLBase ownerSqlBase = (SQLBase)ownerDP.GetDataProvider();
            if (ConnDynamic)
                ownerSqlBase.ReleaseSqlConnection(ownerSqlBase.ConnectionString);
            else if (ownerSqlBase.Conn != null)
                ownerSqlBase.Conn.Close();
            ownerSqlBase.Conn = null;

            // release all dependent dataprovider's connection
            foreach (DataProviderImpl dp in dps) {
                SQLBase sqlBase = (SQLBase)dp.GetDataProvider();
                if (ConnDynamic)
                    sqlBase.ReleaseSqlConnection(sqlBase.ConnectionString);
                else if (ownerSqlBase.Conn != null)
                    sqlBase.Conn.Close();
                sqlBase.Conn = null;
            }

            // Make a new connection
            ownerSqlBase.Conn = new SqlConnection(ownerSqlBase.ConnectionString);
            ownerSqlBase.ConnDynamic = false;

            // all dependent data providers have to use the same connection
            foreach (DataProviderImpl dp in dps) {
                SQLBase sqlBase = (SQLBase)dp.GetDataProvider();
                sqlBase.Conn = ownerSqlBase.Conn;
                sqlBase.ConnDynamic = false;
            }

            if (Trans != null) throw new InternalError("StartTransaction has already been called for this data provider");
            Trans = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = IsolationLevel.Serializable }, TransactionScopeAsyncFlowOption.Enabled);
            return new DataProviderTransaction(CommitTransactionAsync, AbortTransaction);
        }
        /// <summary>
        /// Commits a transaction, saving all updates.
        /// </summary>
        public Task CommitTransactionAsync() {
            if (Trans == null) throw new InternalError("StartTransaction was not called for this data provider - nothing to commit");
            Trans.Complete();//TODO: Asyncify
            Trans.Dispose();
            Trans = null;
            return Task.CompletedTask;
        }
        /// <summary>
        /// Aborts a transaction, abandoning all updates.
        /// </summary>
        public void AbortTransaction() {
            if (Trans != null)
                Trans.Dispose();
            Trans = null;
        }

        // SORTS, FILTERS
        // SORTS, FILTERS
        // SORTS, FILTERS

        internal async Task<string> MakeJoinsAsync(SQLHelper helper, List<JoinData> joins) {
            SQLBuilder sb = new SQLBuilder();
            if (joins != null) {
                foreach (JoinData join in joins) {
                    ISQLTableInfo joinInfo = await join.JoinDP.GetDataProvider().GetISQLTableInfoAsync();
                    string joinTable = joinInfo.GetTableName();
                    ISQLTableInfo mainInfo = await join.MainDP.GetDataProvider().GetISQLTableInfoAsync();
                    string mainTable = mainInfo.GetTableName();
                    if (join.JoinType == JoinData.JoinTypeEnum.Left)
                        sb.Add($"LEFT JOIN {joinTable}");
                    else
                        sb.Add($"INNER JOIN {joinTable}");
                    sb.Add(" ON ");
                    if (join.UseSite && SiteIdentity > 0)
                        sb.Add("(");
                    sb.Add($"{sb.BuildFullColumnName(mainTable, join.MainColumn)} = {sb.BuildFullColumnName(joinTable, join.JoinColumn)}");
                    if (join.UseSite && SiteIdentity > 0)
                        sb.Add($") AND {sb.BuildFullColumnName(mainTable, SiteColumn)} = {sb.BuildFullColumnName(joinTable, SiteColumn)}");
                }
            }
            return sb.ToString();
        }
        internal string MakeFilter(SQLHelper sqlHelper, List<DataProviderFilterInfo> filters, Dictionary<string, string> visibleColumns = null) {
            SQLBuilder sb = new SQLBuilder();
            if (filters != null && filters.Count() > 0) {
                if (SiteIdentity > 0)
                    sb.Add("WHERE (");
                else
                    sb.Add("WHERE ");
                sqlHelper.AddWhereExpr(sb, Dataset, filters, visibleColumns);
                if (SiteIdentity > 0)
                    sb.Add($") AND {sb.BuildFullColumnName(Database, Dbo, Dataset, SiteColumn)} = {SiteIdentity}");
            } else {
                if (SiteIdentity > 0)
                    sb.Add($"WHERE {sb.BuildFullColumnName(Database, Dbo, Dataset, SiteColumn)} = {SiteIdentity}");
            }
            return sb.ToString();
        }
        internal string MakeColumnList(SQLHelper sqlHelper, Dictionary<string, string> visibleColumns, List<JoinData> joins) {
            SQLBuilder sb = new SQLBuilder();
            if (joins != null && joins.Count > 0) {
                foreach (string col in visibleColumns.Values) {
                    sb.Add($"{col},");
                }
                sb.RemoveLastCharacter();
            } else {
                sb.Add("*");
            }
            return sb.ToString();
        }

        // Visible columns
        // Visible columns
        // Visible columns

        // Flatten the current table(with joins) and create a lookup table for all fields.
        // If a joined table has a field with the same name as the lookup table, it is not accessible.
        internal async Task<Dictionary<string, string>> GetVisibleColumnsAsync(string databaseName, string dbOwner, string tableName, Type objType, List<JoinData> joins) {
            SQLManager sqlManager = new SQLManager();
            Dictionary<string, string> visibleColumns = new Dictionary<string, string>();
            tableName = tableName.Trim(new char[] { '[', ']' });
            List<string> columns = sqlManager.GetColumnsOnly(Conn, databaseName, dbOwner, tableName);
            AddVisibleColumns(visibleColumns, databaseName, dbOwner, tableName, columns);
            if (CalculatedPropertyCallbackAsync != null) {
                List<PropertyData> props = ObjectSupport.GetPropertyData(objType);
                props = (from p in props where p.CalculatedProperty select p).ToList();
                foreach (PropertyData prop in props)
                    visibleColumns.Add(prop.ColumnName, prop.ColumnName);
            }
            if (joins != null) {
                // no support for calculated properties in joined tables
                foreach (JoinData join in joins) {
                    ISQLTableInfo mainInfo = await join.MainDP.GetDataProvider().GetISQLTableInfoAsync();
                    databaseName = mainInfo.GetDatabaseName();
                    dbOwner = mainInfo.GetDbOwner();
                    tableName = mainInfo.GetTableName();
                    tableName = tableName.Split(new char[] { '.' }).Last().Trim(new char[] { '[', ']' });
                    columns = sqlManager.GetColumnsOnly(Conn, databaseName, dbOwner, tableName);
                    AddVisibleColumns(visibleColumns, databaseName, dbOwner, tableName, columns);
                    ISQLTableInfo joinInfo = await join.JoinDP.GetDataProvider().GetISQLTableInfoAsync();
                    databaseName = joinInfo.GetDatabaseName();
                    dbOwner = joinInfo.GetDbOwner();
                    tableName = joinInfo.GetTableName();
                    tableName = tableName.Split(new char[] { '.' }).Last().Trim(new char[] { '[', ']' });
                    columns = sqlManager.GetColumnsOnly(join.JoinDP.GetDataProvider().Conn, databaseName, dbOwner, tableName);
                    AddVisibleColumns(visibleColumns, databaseName, dbOwner, tableName, columns);
                }
            }
            return visibleColumns;
        }
        private void AddVisibleColumns(Dictionary<string, string> visibleColumns, string databaseName, string dbOwner, string tableName, List<string> columns) {
            SQLBuilder sb = new SQLBuilder();
            foreach (string column in columns) {
                if (!visibleColumns.ContainsKey(column))
                    visibleColumns.Add(column, sb.BuildFullColumnName(databaseName, dbOwner, tableName, column));
            }
        }

        internal async Task<string> CalculatedPropertiesAsync(Type objType) {
            if (CalculatedPropertyCallbackAsync == null) return null;
            SQLBuilder sb = new SQLBuilder();
            List<PropertyData> props = ObjectSupport.GetPropertyData(objType);
            props = (from p in props where p.CalculatedProperty select p).ToList();
            foreach (PropertyData prop in props) {
                string calcProp = await CalculatedPropertyCallbackAsync(prop.Name);
                sb.Add($", ({calcProp}) AS [{prop.Name}]");
            }
            return sb.ToString();
        }
        internal string GetColumnList(List<PropertyData> propData, Type tpContainer,
                string prefix, bool topMost,
                bool SiteSpecific = false,
                bool WithDerivedInfo = false,
                bool SubTable = false) {
            SQLBuilder sb = new SQLBuilder();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    string colName = prop.ColumnName;
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        sb.Add($"[{prefix}{colName}],");
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                        foreach (LanguageData lang in Languages)
                            sb.Add($"[{prefix}{ColumnFromPropertyWithLanguage(lang.Id, colName)}],");
                    } else if (pi.PropertyType == typeof(Image)) {
                        sb.Add($"[{prefix}{colName}],");
                    } else if (TryGetDataType(pi.PropertyType)) {
                        sb.Add($"[{prefix}{colName}],");
                    } else if (pi.PropertyType.IsClass /* && propmmd.Model != null*/ && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // This is a enumerated type, so we have to create separate values using this table's identity column as a link
                        ; // these values are added as a subtable
                    } else if (pi.PropertyType.IsClass /*&& propmmd.Model != null*/) {
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(pi.PropertyType);
                        string columns = GetColumnList(subPropData, pi.PropertyType, prefix + colName + "_", false, SiteSpecific: false);
                        if (columns.Length > 0) {
                            sb.Add($"{columns},");
                        }
                    } else
                        throw new InternalError("Unknown property type {2} used in class {0}, property {1}", tpContainer.FullName, colName, pi.PropertyType.FullName);
                }
            }
            if (SiteSpecific) {
                sb.Add($"[{prefix}{SiteColumn}],");
            }
            if (WithDerivedInfo) {
                sb.Add($"[{prefix}DerivedDataTableName],");
                sb.Add($"[{prefix}DerivedDataType],");
                sb.Add($"[{prefix}DerivedAssemblyName],");
            }
            if (SubTable) {
                sb.Add($"[{prefix}{SubTableKeyColumn}],");
            }
            sb.RemoveLastCharacter();// ,
            return sb.ToString();
        }
        internal string GetValueList(SQLHelper sqlHelper, string tableName, object container, List<PropertyData> propData, Type tpContainer,
                string prefix = "", bool topMost = false,
                bool SiteSpecific = false,
                Type DerivedType = null, string DerivedTableName = null,
                bool SubTable = false) {
            SQLBuilder sb = new SQLBuilder();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    string colName = prop.ColumnName;
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        object val = pi.GetValue(container);
                        if (val != null) {
                            if (pi.PropertyType == typeof(byte[])) {
                                sb.Add(sqlHelper.AddTempParam(val));
                            } else {
                                byte[] data = new GeneralFormatter().Serialize(val);
                                sb.Add(sqlHelper.AddTempParam(data));
                            }
                        } else {
                            sb.Add(sqlHelper.AddNullTempParam());
                        }
                        sb.Add(",");
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                        MultiString ms = (MultiString)pi.GetValue(container);
                        foreach (LanguageData lang in Languages) {
                            sb.Add(sqlHelper.AddTempParam(ms[lang.Id] ?? ""));
                            sb.Add(",");
                        }
                    } else if (pi.PropertyType == typeof(Image)) {
                        object val = pi.GetValue(container);
                        BinaryFormatter binaryFmt = new BinaryFormatter { AssemblyFormat = 0/*System.Runtime.Serialization.Formatters.FormatterAssemblyStyle.Simple*/ };
                        using (MemoryStream ms = new MemoryStream()) {
                            binaryFmt.Serialize(ms, val);
                            sb.Add(sqlHelper.AddTempParam(ms.ToArray()));
                        }
                        sb.Add(",");
                    } else if (pi.PropertyType == typeof(TimeSpan)) {
                        TimeSpan val = (TimeSpan)pi.GetValue(container);
                        long ticks = val.Ticks;
                        sb.Add(sqlHelper.AddTempParam(ticks));
                        sb.Add(",");
                    } else if (TryGetDataType(pi.PropertyType)) {
                        sb.Add(sqlHelper.AddTempParam(pi.GetValue(container)));
                        sb.Add(",");
                    } else if (pi.PropertyType.IsClass /* && propmmd.Model != null*/ && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // This is a enumerated type, so we have to create separate values using this table's identity column as a link
                        // determine the enumerated type
                        // none
                    } else if (pi.PropertyType.IsClass) {
                        object objVal = pi.GetValue(container);
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(pi.PropertyType);
                        string values = GetValueList(sqlHelper, tableName, objVal, subPropData, pi.PropertyType, prefix + prop.Name + "_", false);
                        if (values.Length > 0) {
                            sb.Add(values);
                            sb.Add(",");
                        }
                    } else
                        throw new InternalError("Unknown property type {2} used in class {0}, property {1}", tpContainer.FullName, prop.Name, prop.PropInfo.PropertyType.FullName);
                }
            }
            if (SiteSpecific) {
                sb.Add(sqlHelper.AddTempParam(SiteIdentity));
                sb.Add(",");
            }
            if (DerivedType != null) {
                if (DerivedTableName == null) throw new InternalError("Missing DerivedTableName");
                sb.Add(sqlHelper.AddTempParam(DerivedTableName));
                sb.Add(",");
                sb.Add(sqlHelper.AddTempParam(DerivedType.FullName));
                sb.Add(",");
                sb.Add(sqlHelper.AddTempParam(DerivedType.Assembly.FullName.Split(new char[] { ',' }, 2).First()));
                sb.Add(",");
            }
            if (SubTable) {
                sb.Add("@__IDENTITY,");
            }
            sb.RemoveLastCharacter();// ,
            return sb.ToString();
        }

        internal string SetColumns(SQLHelper sqlHelper, string tableName, List<PropertyData> propData, object container, Type tpContainer, string prefix = "", bool topMost = false, bool SiteSpecific = false) {
            SQLBuilder sb = new SQLBuilder();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    string colName = prop.ColumnName;
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        object val = pi.GetValue(container);
                        if (pi.PropertyType == typeof(byte[])) {
                            sb.Add(sqlHelper.Expr(prefix + colName, "=", val, true));
                        } else if (val == null) {
                            sb.Add(sqlHelper.Expr(prefix + colName, "=", null, true));
                        } else {
                            byte[] data = new GeneralFormatter().Serialize(val);
                            sb.Add(sqlHelper.Expr(prefix + colName, "=", data, true));
                        }
                        sb.Add(",");
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                        MultiString ms = (MultiString)pi.GetValue(container);
                        foreach (LanguageData lang in Languages) {
                            sb.Add(sqlHelper.Expr(prefix + ColumnFromPropertyWithLanguage(lang.Id, colName), "=", ms[lang.Id], true));
                            sb.Add(",");
                        }
                    } else if (pi.PropertyType == typeof(Image)) {
                        object val = pi.GetValue(container);
                        BinaryFormatter binaryFmt = new BinaryFormatter { AssemblyFormat = 0/*System.Runtime.Serialization.Formatters.FormatterAssemblyStyle.Simple*/ };
                        using (MemoryStream ms = new MemoryStream()) {
                            binaryFmt.Serialize(ms, val);
                            sb.Add(sqlHelper.Expr(prefix + colName, "=", ms.ToArray(), true));
                            sb.Add(",");
                        }
                    } else if (pi.PropertyType == typeof(TimeSpan)) {
                        TimeSpan val = (TimeSpan)pi.GetValue(container);
                        long ticks = val.Ticks;
                        sb.Add(sqlHelper.Expr(prefix + colName, "=", ticks, true));
                        sb.Add(",");
                    } else if (TryGetDataType(pi.PropertyType)) {
                        sb.Add(sqlHelper.Expr(prefix + colName, "=", pi.GetValue(container), true));
                        sb.Add(",");
                    } else if (pi.PropertyType.IsClass && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // This is a enumerated type, saved in a separate table
                    } else if (pi.PropertyType.IsClass) {
                        object objVal = pi.GetValue(container);
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(pi.PropertyType);
                        sb.Add(SetColumns(sqlHelper, tableName, subPropData, objVal, pi.PropertyType, prefix + colName + "_", false));
                        sb.Add(",");
                    } else
                        throw new InternalError("Unknown property type {2} used in class {0}, property {1}", tpContainer.FullName, colName, pi.PropertyType.FullName);
                }
            }
            if (SiteSpecific) {
                sb.Add(sqlHelper.Expr(prefix + SiteColumn, "=", SiteIdentity, true));
                sb.Add(",");
            }
            sb.RemoveLastCharacter();
            return sb.ToString();
        }

        // DIRECT
        // DIRECT
        // DIRECT

        /// <summary>
        /// Executes the provided SQL statement(s) and returns a scalar integer.
        /// </summary>
        /// <param name="sql">The SQL statement(s).</param>
        /// <returns>Returns a scalar integer.</returns>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to SQL repositories.</remarks>
        public async Task<int> Direct_ScalarIntAsync(string sql) {
            await EnsureOpenAsync();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            string tableName = GetTableName();
            sql = sql.Replace("{TableName}", SQLBuilder.WrapIdentifier(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $"[{SiteColumn}] = {SiteIdentity}");
            object o = await sqlHelper.ExecuteScalarAsync(sql);
            if (o == null || o.GetType() == typeof(System.DBNull))
                return 0;
            return Convert.ToInt32(o);
        }
        /// <summary>
        /// Executes the provided SQL statement(s).
        /// </summary>
        /// <param name="sql">The SQL statement(s).</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to SQL repositories.
        ///
        /// When using arguments, they are referenced in the SQL statement(s) <paramref name="sql"/> using @p1, @p2, etc. where @p1 is replaced by the first optional argument.
        /// SQL injection attacks are not possible when using parameters.
        /// </remarks>
        public async Task Direct_QueryAsync(string sql) {
            await EnsureOpenAsync();
            await Direct_QueryAsync(sql, Array.Empty<object>());
        }
        /// <summary>
        /// Executes the provided SQL statement(s).
        /// </summary>
        /// <param name="sql">The SQL statement(s).</param>
        /// <param name="args">Optional arguments that are passed when executing the SQL statements.</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to SQL repositories.
        ///
        /// When using arguments, they are referenced in the SQL statement(s) <paramref name="sql"/> using @p1, @p2, etc. where @p1 is replaced by the first optional argument.
        /// SQL injection attacks are not possible when using parameters.
        /// </remarks>
        public async Task Direct_QueryAsync(string sql, params object[] args) {
            await EnsureOpenAsync();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            string tableName = GetTableName();
            int count = 0;
            foreach (object arg in args) {
                ++count;
                sqlHelper.AddParam($"p{count}", arg);
            }
            sql = sql.Replace("{TableName}", SQLBuilder.WrapIdentifier(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $"[{SiteColumn}] = {SiteIdentity}");
            await sqlHelper.ExecuteNonQueryAsync(sql);
        }
        /// <summary>
        /// Executes the provided SQL statement(s) and returns an object of type {i}TYPE{/i}.
        /// </summary>
        /// <param name="sql">The SQL statement(s).</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to SQL repositories.</remarks>
        /// <returns>Returns an object of type {i}TYPE{/i}.</returns>
        public async Task<TYPE> Direct_QueryAsync<TYPE>(string sql) {
            return await Direct_QueryAsync<TYPE>(sql, Array.Empty<object>());
        }
        /// <summary>
        /// Executes the provided SQL statement(s) and returns an object of type {i}TYPE{/i}.
        /// </summary>
        /// <param name="sql">The SQL statement(s).</param>
        /// <param name="args">Optional arguments that are passed when executing the SQL statements.</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to SQL repositories.</remarks>
        /// <returns>Returns an object of type {i}TYPE{/i}.</returns>
        public async Task<TYPE> Direct_QueryAsync<TYPE>(string sql, params object[] args) {
            await EnsureOpenAsync();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            string tableName = GetTableName();
            int count = 0;
            foreach (object arg in args) {
                ++count;
                sqlHelper.AddParam($"p{count}", arg);
            }
            sql = sql.Replace("{TableName}", SQLBuilder.WrapIdentifier(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $"[{SiteColumn}] = {SiteIdentity}");
            List<TYPE> list = new List<TYPE>();
            using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(sql)) {
                if (YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())
                    return sqlHelper.CreateObject<TYPE>(reader);
                else
                    return default(TYPE);
            }
        }
        /// <summary>
        /// Executes the provided SQL statement(s) and returns a collection of objects (one for each row retrieved) of type {i}TYPE{/i}.
        /// </summary>
        /// <param name="sql">The SQL statement(s).</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to SQL repositories.</remarks>
        /// <returns>Returns a collection  of objects (one for each row retrieved) of type {i}TYPE{/i}.</returns>
        public async Task<List<TYPE>> Direct_QueryListAsync<TYPE>(string sql) {
            return await Direct_QueryListAsync<TYPE>(sql, Array.Empty<object>());
        }
        /// <summary>
        /// Executes the provided SQL statement(s) and returns a collection of objects (one for each row retrieved) of type {i}TYPE{/i}.
        /// </summary>
        /// <param name="sql">The SQL statement(s).</param>
        /// <param name="args">Optional arguments that are passed when executing the SQL statements.</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to SQL repositories.</remarks>
        /// <returns>Returns a collection of objects (one for each row retrieved) of type {i}TYPE{/i}.</returns>
        public async Task<List<TYPE>> Direct_QueryListAsync<TYPE>(string sql, params object[] args) {
            await EnsureOpenAsync();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            string tableName = GetTableName();
            int count = 0;
            foreach (object arg in args) {
                ++count;
                sqlHelper.AddParam($"p{count}", arg);
            }
            sql = sql.Replace("{TableName}", SQLBuilder.WrapIdentifier(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $"[{SiteColumn}] = {SiteIdentity}");
            List<TYPE> list = new List<TYPE>();
            using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(sql)) {
                while (YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())
                    list.Add(sqlHelper.CreateObject<TYPE>(reader));
            }
            return list;
        }

        /// <summary>
        /// Executes the provided SQL statement(s) and returns a paged collection of objects (one for each row retrieved) of type {i}TYPE{/i}.
        /// </summary>
        /// <param name="sql">The SQL statement(s).</param>
        /// <param name="skip">The number of records to skip (paging support).</param>
        /// <param name="take">The number of records to retrieve (paging support). If more records are available they are dropped.</param>
        /// <param name="sort">A collection describing the sort order.</param>
        /// <param name="filters">A collection describing the filtering criteria.</param>
        /// <param name="args">Optional arguments that are passed when executing the SQL statements.</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to SQL repositories.</remarks>
        /// <returns>Returns a collection  of objects (one for each row retrieved) of type {i}TYPE{/i}.</returns>
        /// <remarks>
        /// $WhereFilter$ and $OrderBy$ embedded in the SQL statements are replace with a complete WHERE clause for filtering and the column names for sorting, respectively.
        ///
        /// The SQL statements must create two result sets. The first, a scalar value with the total number of records (not paged) and the second result set is a collection of objects of type {i}TYPE{/i}.
        /// </remarks>
        public async Task<DataProviderGetRecords<TYPE>> Direct_QueryPagedListAsync<TYPE>(string sql, int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, params object[] args) {
            await EnsureOpenAsync();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            SQLBuilder sb = new SQLBuilder();

            string tableName = GetTableName();
            int count = 0;
            foreach (object arg in args) {
                ++count;
                sqlHelper.AddParam($"p{count}", arg);
            }
            sql = sql.Replace("{TableName}", SQLBuilder.WrapIdentifier(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $"[{SiteColumn}] = {SiteIdentity}");

            sort = NormalizeSort(typeof(TYPE), sort);
            sql = sql.Replace("$OrderBy$", sb.GetOrderBy(null, sort, Offset: skip, Next: take));

            filters = NormalizeFilter(typeof(TYPE), filters);
            string filter = MakeFilter(sqlHelper, filters, null);
            sql = sql.Replace("$WhereFilter$", filter);
            sql += "\n\n" + sqlHelper.DebugInfo;

            DataProviderGetRecords<TYPE> recs = new DataProviderGetRecords<TYPE>();

            using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(sql)) {

                if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) throw new InternalError("Expected # of records");
                recs.Total = reader.GetInt32(0);
                if (!(YetaWFManager.IsSync() ? reader.NextResult() : await reader.NextResultAsync())) throw new InternalError("Expected next result set (main table)");

                while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) {
                    TYPE o = sqlHelper.CreateObject<TYPE>(reader);
                    recs.Data.Add(o);
                    // no subtables
                }
            }
            return recs;
        }

        /// <summary>
        /// Executes the provided SQL stored procedure returns a collection of objects (one for each row retrieved) of type {i}TYPE{/i}.
        /// </summary>
        /// <param name="sqlProc">The name of the stored procedure.</param>
        /// <param name="parms">An anonymous object with named parameters.</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to SQL repositories.</remarks>
        /// <returns>Returns a collection of objects (one for each row retrieved) of type {i}TYPE{/i}.</returns>
        public async Task<DataProviderGetRecords<TYPE>> Direct_StoredProcAsync<TYPE>(string sqlProc, object parms = null) {
            await EnsureOpenAsync();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            if (parms != null) {
                foreach (PropertyInfo propertyInfo in parms.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public)) {
                    sqlHelper.AddParam($"@{propertyInfo.Name}", propertyInfo.GetValue(parms, null));
                }
            }

            DataProviderGetRecords<TYPE> recs = new DataProviderGetRecords<TYPE>();
            using (SqlDataReader reader = await sqlHelper.ExecuteReaderStoredProcAsync(sqlProc)) {

                while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) {
                    TYPE o = sqlHelper.CreateObject<TYPE>(reader);
                    recs.Data.Add(o);
                    // no subtables
                }
                recs.Total = recs.Data.Count;
            }
            return recs;
        }

        // ISQLTableInfo
        // ISQLTableInfo
        // ISQLTableInfo

        /// <summary>
        /// Returns an ISQLTableInfo interface for the data provider.
        /// </summary>
        /// <returns>Returns an ISQLTableInfo interface for the data provider.</returns>
        public async Task<ISQLTableInfo> GetISQLTableInfoAsync() {
            await EnsureOpenAsync();
            return (ISQLTableInfo)this;
        }

        /// <summary>
        /// Returns the connection string used by the data provider.
        /// </summary>
        /// <returns>Returns the connection string used by the data provider.</returns>
        public string GetConnectionString() {
            return ConnectionString;
        }
        /// <summary>
        /// Returns the database name used by the data provider.
        /// </summary>
        /// <returns>Returns the database name used by the data provider.</returns>
        public string GetDatabaseName() {
            return Database;
        }
        /// <summary>
        /// Returns the database owner used by the data provider.
        /// </summary>
        /// <returns>Returns the database owner used by the data provider.</returns>
        public string GetDbOwner() {
            return Dbo;
        }
        /// <summary>
        /// Returns the table name used by the data provider.
        /// </summary>
        /// <returns>Returns the table name used by the data provider.</returns>
        public string GetTableName() {
            SQLBuilder sb = new SQLBuilder();
            return sb.BuildFullTableName(Database, Dbo, Dataset);
        }
        /// <summary>
        /// Replaces search text in a SQL string fragment with the table name used by the data provider.
        /// </summary>
        /// <param name="text">A SQL fragment where occurrences of <paramref name="searchText"/> are replaced by the table name used by the data provider.</param>
        /// <param name="searchText">The text searched in <paramref name="text"/> that is replaced by the table name used by the data provider.</param>
        /// <returns>Returns the SQL string fragment with <paramref name="searchText"/> replaced by the table name used by the data provider.</returns>
        public string ReplaceWithTableName(string text, string searchText) {
            return text.Replace(searchText, GetTableName());
        }
        /// <summary>
        /// Replaces search text in a SQL string fragment with the language used by the data provider.
        /// </summary>
        /// <param name="text">A SQL fragment where occurrences of <paramref name="searchText"/> are replaced by the language used by the data provider.</param>
        /// <param name="searchText">The text searched in <paramref name="text"/> that is replaced by the language used by the data provider.</param>
        /// <returns>Returns the SQL string fragment with <paramref name="searchText"/> replaced by the language used by the data provider.</returns>
        public string ReplaceWithLanguage(string text, string searchText) {
            return text.Replace(searchText, GetLanguageSuffix());
        }
    }
}
