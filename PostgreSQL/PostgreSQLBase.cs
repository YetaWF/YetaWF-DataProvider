/* Copyright © 2019 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Npgsql;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading.Tasks;
using System.Transactions;
using YetaWF.Core.Components;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Language;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Support;
using YetaWF.Core.Support.Serializers;

namespace YetaWF.DataProvider.PostgreSQL {

    /// <summary>
    /// This abstract class is the base class for all PostgreSQL low-level data providers.
    /// </summary>
    public abstract class PostgreSQLBase : IDisposable, IDataProviderTransactions {

        /// <summary>
        /// Defines the name of the PostgreSQL low-level data provider.
        /// This name is used in appsettings.json ("IOMode": "PostgreSQL").
        /// </summary>
        public const string ExternalName = "PostgreSQL";
        /// <summary>
        /// Defines the key used in appsettings.json to define a PostgreSQL connection string
        /// ("PostgreSQLConnect": "Data Source=...datasource...;Initial Catalog=...catalog...;User ID=..userid..;Password=..password..").
        /// </summary>
        public const string PostgreSQLConnectString = "PostgreSQLConnect";

        private const string DefaultString = "Default";
        private const string PostgreSQLSchemaString = "PostgreSQLSchema";

        /// <summary>
        /// Defines the column name used to associate a site with a data record. The __Site column contains the site ID, or 0 if there is no associated site.
        /// Not all tables use the __Site column.
        /// </summary>
        public const string SiteColumn = "__Site";
        /// <summary>
        /// Defines the column name of the identity column used in tables. Not all tables use an identity column.
        /// </summary>
        public const string IdentityColumn = "Identity";
        /// <summary>
        /// Defines the column name in subtables to connect a subtable and its records to the main table.
        /// The __Key column in a subtable contains the identity column used in the main table, used to join record data across tables.
        /// </summary>
        public const string SubTableKeyColumn = "__Key";

        /// <summary>
        /// A dictionary of options and optional parameters as provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider was created.
        /// </summary>
        public Dictionary<string, object> Options { get; private set; }
        /// <summary>
        /// The package implementing the data provider.
        /// </summary>
        public Package Package { get; private set; }

        /// <summary>
        /// The section in appsettings.json, where PostgreSQL connection string, database owner, etc. are located.
        /// WebConfigArea is normally not specified and all connection information is derived from the appsettings.json section that corresponds to the table name used by the data provider.
        /// This can be overridden by passing an optional WebConfigArea parameter to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.
        /// </summary>
        /// <remarks>This is not used by application data providers. Only the YetaWF.DataProvider.ModuleDefinitionDataProvider uses this feature.</remarks>
        public string WebConfigArea { get; private set; }
        /// <summary>
        /// The dataset provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider was created.
        /// </summary>
        public string Dataset { get; protected set; }
        /// <summary>
        /// The database used by this data provider. This information is extracted from the PostgreSQL connection string.
        /// </summary>
        public string Database { get; private set; }
        /// <summary>
        /// The site identity provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider was created.
        ///
        /// This may be 0 if no specific site is associated with the data provider.
        /// </summary>
        public int SiteIdentity { get; private set; }
        /// <summary>
        /// The initial value of the identity seed. The default value is defined by YetaWF.Core.DataProvider.DataProviderImpl.IDENTITY_SEED, but this can be overridden by passing an
        /// optional IdentitySeed parameter to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.
        /// </summary>
        public int IdentitySeed { get; private set; }
        /// <summary>
        /// Defines whether the data is cacheable.
        /// This corresponds to the Cacheable parameter of the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method.
        /// </summary>
        public bool Cacheable { get; private set; }
        /// <summary>
        /// Defines whether logging is wanted for the data provider. The default value is false, but this can be overridden by passing an
        /// optional Logging parameter to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.
        /// </summary>
        public bool Logging { get; private set; }
        /// <summary>
        /// Defines whether language support (for YetaWF.Core.Models.MultiString) is wanted for the data provider. The default is true. This can be overridden by passing an
        /// optional NoLanguages parameter to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.
        /// </summary>
        public bool NoLanguages { get; private set; }
        /// <summary>
        /// Defines the languages supported by the data provider. If NoLanguages is true, no language data is available.
        /// Otherwise, the languages supported are identical to collection of active languages defined by YetaWF.Core.Models.MultiString.Languages.
        /// </summary>
        public List<LanguageData> Languages { get; private set; }

        /// <summary>
        /// An optional callback which is called whenever an object is retrieved to update some properties.
        /// </summary>
        /// <remarks>
        /// Properties that are derived from other property values are considered "calculated properties". This callback
        /// is called after retrieving an object to update these properties.
        ///
        /// This callback is typically set by the data provider itself, in its constructor or as the data provider is being created.
        /// </remarks>
        protected Func<string, Task<string>> CalculatedPropertyCallbackAsync { get; set; }

        /// <summary>
        /// Defines the PostgreSQL connection string used by this data provider.
        /// </summary>
        /// <remarks>The PostgreSQL connection string is defined in appsettings.json but may be modified by the data provider.
        /// The ConnectionString property contains the actual connection string used to connect to the PostgreSQL database.
        /// </remarks>
        public string ConnectionString { get; private set; }
        /// <summary>
        /// Defines the database schema used by this data provider.
        /// </summary>
        /// <remarks>The database schema is defined in appsettings.json ("PostgreSQLSchema": "public").
        /// </remarks>
        public string Schema { get; private set; }
        /// <summary>
        /// The underlying Nqgsql.NpgsqlConnection object used to connect to the database.
        /// </summary>
        public NpgsqlConnection Conn { get; private set; }

        internal string AndSiteIdentity { get; private set; }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="options">A dictionary of options and optional parameters as provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.</param>
        /// <remarks>
        /// Data providers are instantiated when the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method is called, usually by an application data provider.
        ///
        /// For debugging purposes, instances of this class are tracked using the DisposableTracker class.
        /// </remarks>
        protected PostgreSQLBase(Dictionary<string, object> options) {
            Options = options;
            if (!Options.ContainsKey("Package") || !(Options["Package"] is Package))
                throw new InternalError($"No Package for data provider {GetType().FullName}");
            Package = (Package)Options["Package"];
            if (!Options.ContainsKey("Dataset") || string.IsNullOrWhiteSpace((string)Options["Dataset"]))
                throw new InternalError($"No Dataset for data provider {GetType().FullName}");
            Dataset = (string)Options["Dataset"];
            if (Options.ContainsKey("SiteIdentity") && Options["SiteIdentity"] is int)
                SiteIdentity = Convert.ToInt32(Options["SiteIdentity"]);
            if (Options.ContainsKey("IdentitySeed") && Options["IdentitySeed"] is int)
                IdentitySeed = Convert.ToInt32(Options["IdentitySeed"]);
            else
                IdentitySeed = DataProviderImpl.IDENTITY_SEED;
            if (Options.ContainsKey("Cacheable") && Options["Cacheable"] is bool)
                Cacheable = Convert.ToBoolean(Options["Cacheable"]);
            if (Options.ContainsKey("Logging") && Options["Logging"] is bool)
                Logging = Convert.ToBoolean(Options["Logging"]);
            else
                Logging = true;
            if (Options.ContainsKey("NoLanguages") && Options["NoLanguages"] is bool)
                NoLanguages = Convert.ToBoolean(Options["NoLanguages"]);

            if (Options.ContainsKey("WebConfigArea"))
                WebConfigArea = (string)Options["WebConfigArea"];

            NpgsqlConnectionStringBuilder sqlsb = new NpgsqlConnectionStringBuilder(GetPostgreSqlConnectionString());
            //$$$ sqlsb.MultipleActiveResultSets = true;
            ConnectionString = sqlsb.ToString();
            Schema = GetPostgreSqlSchema();

            if (NoLanguages)
                Languages = new List<LanguageData>();
            else {
                Languages = MultiString.Languages;
                if (Languages.Count == 0) throw new InternalError("We need Languages");
            }
            if (SiteIdentity > 0)
                AndSiteIdentity = $"AND [{SiteColumn}] = {SiteIdentity}";

            Conn = new NpgsqlConnection(ConnectionString);
            Conn.Open();
            Database = Conn.Database;

            DisposableTracker.AddObject(this);
        }
        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose() { Dispose(true); }
        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="disposing">true to close the database connection and release the DisposableTracker reference count, false otherwise.</param>
        protected virtual void Dispose(bool disposing) {
            if (disposing) {
                DisposableTracker.RemoveObject(this);
                if (Conn != null) {
                    Conn.Close();
                    Conn.Dispose();
                    Conn = null;
                }
            }
        }

        private string GetPostgreSqlConnectionString() {
            string connString = WebConfigHelper.GetValue<string>(string.IsNullOrWhiteSpace(WebConfigArea) ? Dataset : WebConfigArea, PostgreSQLConnectString);
            if (string.IsNullOrWhiteSpace(connString)) {
                if (string.IsNullOrWhiteSpace(WebConfigArea))
                    connString = WebConfigHelper.GetValue<string>(Package.AreaName, PostgreSQLConnectString);
                if (string.IsNullOrWhiteSpace(connString)) {
                    if (_defaultConnectString == null) {
                        _defaultConnectString = WebConfigHelper.GetValue<string>(DefaultString, PostgreSQLConnectString);
                        if (_defaultConnectString == null)
                            throw new InternalError("No SQLConnect connection string found (also no default)");
                    }
                    connString = _defaultConnectString;
                }
            }
            if (string.IsNullOrWhiteSpace(connString)) throw new InternalError($"No PostgreSQL connection string provided (also no default)");
            return connString;
        }
        private string GetPostgreSqlSchema() {
            string schema = WebConfigHelper.GetValue<string>(string.IsNullOrWhiteSpace(WebConfigArea) ? Dataset : WebConfigArea, PostgreSQLSchemaString);
            if (string.IsNullOrWhiteSpace(schema)) {
                if (string.IsNullOrWhiteSpace(WebConfigArea))
                    schema = WebConfigHelper.GetValue<string>(Package.AreaName, PostgreSQLSchemaString);
                if (string.IsNullOrWhiteSpace(schema)) {
                    if (_defaultSchema == null) {
                        _defaultSchema = WebConfigHelper.GetValue<string>(DefaultString, PostgreSQLSchemaString);
                        if (_defaultSchema == null)
                            throw new InternalError("No PostgreSQLSchema schema found (also no default)");
                    }
                    schema = _defaultSchema;
                }
            }
            if (string.IsNullOrWhiteSpace(schema)) throw new InternalError($"No PostgreSQLSchema schema provided (also no default)");
            return schema;
        }
        private static string _defaultConnectString;
        private static string _defaultSchema;

        /// <summary>
        /// Returns the primary key's column name.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        /// <param name="propertyData">The collection of property information.</param>
        /// <returns> Returns the primary key's column name.</returns>
        /// <remarks>
        /// A primary key is defined in a model by decorating a property with the YetaWF.Core.DataProvider.Attributes.Data_PrimaryKey attribute.
        /// If no primary key is defined for the specified table, an exception occurs.
        /// </remarks>
        protected string GetKey1Name(string tableName, List<PropertyData> propertyData) {
            if (_key1Name == null) {
                // find primary key
                foreach (var prop in propertyData) {
                    if (prop.HasAttribute(Data_PrimaryKey.AttributeName)) {
                        _key1Name = prop.Name;
                        return prop.Name;
                    }
                }
                throw new InternalError("Primary key not defined in table {0}", tableName);
            }
            return _key1Name;
        }
        private string _key1Name;

        /// <summary>
        /// Returns the secondary key's column name.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        /// <param name="propertyData">The collection of property information.</param>
        /// <returns> Returns the secondary key's column name.</returns>
        /// <remarks>
        /// A secondary key is defined in a model by decorating a property with the YetaWF.Core.DataProvider.Attributes.Data_PrimaryKey2 attribute.
        /// If no secondary key is defined for the specified table, an exception occurs.
        /// </remarks>
        protected string GetKey2Name(string tableName, List<PropertyData> propertyData) {
            if (_key2Name == null) {
                // find primary key
                foreach (var prop in propertyData) {
                    if (prop.HasAttribute(Data_PrimaryKey2.AttributeName)) {
                        _key2Name = prop.Name;
                        return prop.Name;
                    }
                }
                throw new InternalError("Second primary key not defined in table {0}", tableName);
            }
            return _key2Name;
        }
        private string _key2Name;

        /// <summary>
        /// Returns the identity column name.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        /// <param name="propertyData">The collection of property information.</param>
        /// <returns>Returns the identity column name.</returns>
        /// <remarks>
        /// An identity column is defined in a model by decorating a property with the YetaWF.Core.DataProvider.Attributes.Data_Identity attribute.
        /// If no identity column is defined for the specified table, an empty string is returned.
        /// </remarks>
        protected string GetIdentityName(string tableName, List<PropertyData> propertyData) {
            if (_identityName == null) {
                // find identity
                foreach (var prop in propertyData) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        _identityName = prop.Name;
                        return _identityName;
                    }
                }
                _identityName = "";
            }
            return _identityName;
        }
        private string _identityName;

        /// <summary>
        /// Returns whether the specified identity name string <paramref name="identityName"/> is a valid identity name.
        /// </summary>
        /// <param name="identityName">A string.</param>
        /// <returns>Returns whether the specified identity name strin <paramref name="identityName"/> is a valid identity name.</returns>
        protected bool HasIdentity(string identityName) {
            return !string.IsNullOrWhiteSpace(identityName);
        }

        /// <summary>
        /// Tests whether a given type is a simple type that can be stored in one column.
        /// </summary>
        /// <param name="tp">The type to test.</param>
        /// <returns>Returns true if the type is a simple type that can be stored in one column, false otherwise.</returns>
        protected static bool TryGetDataType(Type tp) {
            if (tp == typeof(DateTime) || tp == typeof(DateTime?))
                return true;
            else if (tp == typeof(TimeSpan) || tp == typeof(TimeSpan?))
                return true;
            else if (tp == typeof(decimal) || tp == typeof(decimal?))
                return true;
            else if (tp == typeof(bool) || tp == typeof(bool?))
                return true;
            else if (tp == typeof(System.Guid) || tp == typeof(System.Guid?))
                return true;
            else if (tp == typeof(Image))
                return true;
            else if (tp == typeof(int) || tp == typeof(int?))
                return true;
            else if (tp == typeof(long) || tp == typeof(long?))
                return true;
            else if (tp == typeof(Single) || tp == typeof(Single?))
                return true;
            else if (tp == typeof(string))
                return true;
            else if (tp.IsEnum)
                return true;
            return false;
        }

        // IDATAPROVIDERTRANSACTIONS
        // IDATAPROVIDERTRANSACTIONS
        // IDATAPROVIDERTRANSACTIONS

        private TransactionScope Trans { get; set; }

        /// <summary>
        /// Starts a transaction that can be committed, saving all updates, or aborted to abandon all updates.
        /// </summary>
        /// <returns>Returns a YetaWF.Core.DataProvider.DataProviderTransaction object.</returns>
        public DataProviderTransaction StartTransaction() {
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

        // Update column names for constructed names (as used in MultiString)
        internal List<DataProviderFilterInfo> NormalizeFilter(Type type, List<DataProviderFilterInfo> filters) {
            if (filters == null) return null;
            filters = (from f in filters select new DataProviderFilterInfo(f)).ToList();// copy list
            foreach (DataProviderFilterInfo f in filters)
                if (f.Field != null) f.Field = f.Field.Replace(".", "_");
            Grid.NormalizeFilters(type, filters);
            foreach (DataProviderFilterInfo filter in filters) {
                if (filter.Filters != null)
                    filter.Filters = NormalizeFilter(type, filter.Filters);
                else if (!string.IsNullOrWhiteSpace(filter.Field))
                    filter.Field = NormalizeFilter(type, filter);
            }
            return filters;
        }
        private string NormalizeFilter(Type type, DataProviderFilterInfo filter) {
            PropertyInfo prop = ObjectSupport.TryGetProperty(type, filter.Field);
            if (prop == null) return filter.Field; // could be a composite field, like Event.ImplementingAssembly
            if (prop.PropertyType == typeof(MultiString)) {
                MultiString ms = new MultiString(filter.ValueAsString);
                filter.Value = ms.ToString();
                return ColumnFromPropertyWithLanguage(MultiString.ActiveLanguage, filter.Field);
            }
            return filter.Field;
        }
        internal List<DataProviderSortInfo> NormalizeSort(Type type, List<DataProviderSortInfo> sorts) {
            if (sorts == null) return null;
            sorts = (from s in sorts select new DataProviderSortInfo(s)).ToList();// copy list
            foreach (DataProviderSortInfo sort in sorts) {
                PropertyInfo prop = ObjectSupport.GetProperty(type, sort.Field);
                if (prop.PropertyType == typeof(MultiString))
                    sort.Field = ColumnFromPropertyWithLanguage(MultiString.ActiveLanguage, sort.Field);
            }
            return sorts;
        }
        internal static string ColumnFromPropertyWithLanguage(string langId, string field) {
            return field + "_" + langId.Replace("-", "_");
        }
        internal static string GetLanguageSuffix() {
            return ColumnFromPropertyWithLanguage(MultiString.ActiveLanguage, "");
        }
        internal string MakeJoins(PostgreSQLHelper helper, List<JoinData> joins) {
            PostgreSQLBuilder sb = new PostgreSQLBuilder();
            if (joins != null) {
                foreach (JoinData join in joins) {
                    IPostgresSQLTableInfo joinInfo = (IPostgresSQLTableInfo)join.JoinDP.GetDataProvider();
                    string joinTable = joinInfo.GetTableName();
                    IPostgresSQLTableInfo mainInfo = (IPostgresSQLTableInfo)join.MainDP.GetDataProvider();
                    string mainTable = mainInfo.GetTableName();
                    if (join.JoinType == JoinData.JoinTypeEnum.Left)
                        sb.Add($"LEFT JOIN {joinTable}");
                    else
                        sb.Add($"INNER JOIN {joinTable}");
                    sb.Add(" ON ");
                    if (join.UseSite && SiteIdentity > 0)
                        sb.Add("(");
                    sb.Add($"{PostgreSQLBuilder.BuildFullColumnName(mainTable, join.MainColumn)} = {PostgreSQLBuilder.BuildFullColumnName(joinTable, join.JoinColumn)}");
                    if (join.UseSite && SiteIdentity > 0)
                        sb.Add($") AND {PostgreSQLBuilder.BuildFullColumnName(mainTable, SiteColumn)} = {PostgreSQLBuilder.BuildFullColumnName(joinTable, SiteColumn)}");
                }
            }
            return sb.ToString();
        }
        internal string MakeFilter(PostgreSQLHelper sqlHelper, List<DataProviderFilterInfo> filters, Dictionary<string, string> visibleColumns = null) {
            PostgreSQLBuilder sb = new PostgreSQLBuilder();
            if (filters != null && filters.Count() > 0) {
                if (SiteIdentity > 0)
                    sb.Add("WHERE (");
                else
                    sb.Add("WHERE ");
                sqlHelper.AddWhereExpr(sb, Dataset, filters, visibleColumns);
                if (SiteIdentity > 0)
                    sb.Add($") AND {PostgreSQLBuilder.BuildFullColumnName(Database, Schema, Dataset, SiteColumn)} = {SiteIdentity}");
            } else {
                if (SiteIdentity > 0)
                    sb.Add($"WHERE {PostgreSQLBuilder.BuildFullColumnName(Database, Schema, Dataset, SiteColumn)} = {SiteIdentity}");
            }
            return sb.ToString();
        }
        internal string MakeColumnList(PostgreSQLHelper sqlHelper, Dictionary<string, string> visibleColumns, List<JoinData> joins) {
            PostgreSQLBuilder sb = new PostgreSQLBuilder();
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
        internal Dictionary<string, string> GetVisibleColumns(string databaseName, string schema, string tableName, Type objType, List<JoinData> joins) {
            Dictionary<string, string> visibleColumns = new Dictionary<string, string>();
            tableName = tableName.Trim(new char[] { '[', ']' });
            List<string> columns = PostgreSQLCache.GetColumns(Conn, ConnectionString, databaseName, schema, tableName);
            AddVisibleColumns(visibleColumns, databaseName, schema, tableName, columns);
            if (CalculatedPropertyCallbackAsync != null) {
                List<PropertyData> props = ObjectSupport.GetPropertyData(objType);
                props = (from p in props where p.CalculatedProperty select p).ToList();
                foreach (PropertyData prop in props)
                    visibleColumns.Add(prop.Name, prop.Name);
            }
            if (joins != null) {
                // no support for calculated properties in joined tables
                foreach (JoinData join in joins) {
                    IPostgresSQLTableInfo mainInfo = (IPostgresSQLTableInfo)join.MainDP.GetDataProvider();
                    databaseName = mainInfo.GetDatabaseName();
                    schema = mainInfo.GetDbOwner();
                    tableName = mainInfo.GetTableName();
                    tableName = tableName.Split(new char[] { '.' }).Last().Trim(new char[] { '[', ']' });
                    columns = PostgreSQLCache.GetColumns(Conn, ConnectionString, databaseName, schema, tableName);
                    AddVisibleColumns(visibleColumns, databaseName, schema, tableName, columns);
                    IPostgresSQLTableInfo joinInfo = (IPostgresSQLTableInfo)join.JoinDP.GetDataProvider();
                    databaseName = joinInfo.GetDatabaseName();
                    schema = joinInfo.GetDbOwner();
                    tableName = joinInfo.GetTableName();
                    tableName = tableName.Split(new char[] { '.' }).Last().Trim(new char[] { '[', ']' });
                    columns = PostgreSQLCache.GetColumns(join.JoinDP.GetDataProvider().Conn, join.JoinDP.GetDataProvider().ConnectionString, databaseName, schema, tableName);
                    AddVisibleColumns(visibleColumns, databaseName, schema, tableName, columns);
                }
            }
            return visibleColumns;
        }
        private void AddVisibleColumns(Dictionary<string, string> visibleColumns, string databaseName, string dbOwner, string tableName, List<string> columns) {
            foreach (string column in columns) {
                if (!visibleColumns.ContainsKey(column))
                    visibleColumns.Add(column, PostgreSQLBuilder.BuildFullColumnName(databaseName, dbOwner, tableName, column));
            }
        }

        internal async Task<string> CalculatedPropertiesAsync(Type objType) {
            if (CalculatedPropertyCallbackAsync == null) return null;
            PostgreSQLBuilder sb = new PostgreSQLBuilder();
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
            PostgreSQLBuilder sb = new PostgreSQLBuilder();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        sb.Add($"\"{prefix}{prop.Name}\",");
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                        foreach (var lang in Languages)
                            sb.Add($"\"{prefix}{ColumnFromPropertyWithLanguage(lang.Id, prop.Name)}\",");
                    } else if (pi.PropertyType == typeof(Image)) {
                        sb.Add($"\"{prefix}{prop.Name}\",");
                    } else if (TryGetDataType(pi.PropertyType)) {
                        sb.Add($"\"{prefix}{prop.Name}\",");
                    } else if (pi.PropertyType.IsClass /* && propmmd.Model != null*/ && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // This is a enumerated type, so we have to create separate values using this table's identity column as a link
                        ; // these values are added as a subtable
                    } else if (pi.PropertyType.IsClass /*&& propmmd.Model != null*/) {
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(pi.PropertyType);
                        string columns = GetColumnList(subPropData, pi.PropertyType, prefix + prop.Name + "_", false, SiteSpecific: false);
                        if (columns.Length > 0) {
                            sb.Add($"{columns},");
                        }
                    } else
                        throw new InternalError("Unknown property type {2} used in class {0}, property {1}", tpContainer.FullName, prop.Name, pi.PropertyType.FullName);
                }
            }
            if (SiteSpecific) {
                sb.Add($"\"{prefix}{SiteColumn}\",");
            }
            if (WithDerivedInfo) {
                sb.Add($"\"{prefix}DerivedDataTableName\",");
                sb.Add($"\"{prefix}DerivedDataType\",");
                sb.Add($"\"{prefix}DerivedAssemblyName\",");
            }
            if (SubTable) {
                sb.Add($"[{prefix}{SubTableKeyColumn}\",");
            }
            sb.RemoveLastCharacter();// ,
            return sb.ToString();
        }
        internal string GetValueList(PostgreSQLHelper sqlHelper, string tableName, object container, List<PropertyData> propData, Type tpContainer,
                string prefix = "", bool topMost = false,
                bool SiteSpecific = false,
                Type DerivedType = null, string DerivedTableName = null,
                bool SubTable = false) {
            PostgreSQLBuilder sb = new PostgreSQLBuilder();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
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
                        foreach (var lang in Languages) {
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

        internal string SetColumns(PostgreSQLHelper sqlHelper, string tableName, List<PropertyData> propData, object container, Type tpContainer, string prefix = "", bool topMost = false, bool SiteSpecific = false) {
            PostgreSQLBuilder sb = new PostgreSQLBuilder();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        object val = pi.GetValue(container);
                        if (pi.PropertyType == typeof(byte[])) {
                            sb.Add(sqlHelper.Expr(prefix + prop.Name, "=", val, true));
                        } else if (val == null) {
                            sb.Add(sqlHelper.Expr(prefix + prop.Name, "=", null, true));
                        } else {
                            byte[] data = new GeneralFormatter().Serialize(val);
                            sb.Add(sqlHelper.Expr(prefix + prop.Name, "=", data, true));
                        }
                        sb.Add(",");
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                        MultiString ms = (MultiString)pi.GetValue(container);
                        foreach (var lang in Languages) {
                            sb.Add(sqlHelper.Expr(prefix + ColumnFromPropertyWithLanguage(lang.Id, prop.Name), "=", ms[lang.Id], true));
                            sb.Add(",");
                        }
                    } else if (pi.PropertyType == typeof(Image)) {
                        object val = pi.GetValue(container);
                        BinaryFormatter binaryFmt = new BinaryFormatter { AssemblyFormat = 0/*System.Runtime.Serialization.Formatters.FormatterAssemblyStyle.Simple*/ };
                        using (MemoryStream ms = new MemoryStream()) {
                            binaryFmt.Serialize(ms, val);
                            sb.Add(sqlHelper.Expr(prefix + prop.Name, "=", ms.ToArray(), true));
                            sb.Add(",");
                        }
                    } else if (pi.PropertyType == typeof(TimeSpan)) {
                        TimeSpan val = (TimeSpan)pi.GetValue(container);
                        long ticks = val.Ticks;
                        sb.Add(sqlHelper.Expr(prefix + prop.Name, "=", ticks, true));
                        sb.Add(",");
                    } else if (TryGetDataType(pi.PropertyType)) {
                        sb.Add(sqlHelper.Expr(prefix + prop.Name, "=", pi.GetValue(container), true));
                        sb.Add(",");
                    } else if (pi.PropertyType.IsClass && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // This is a enumerated type, saved in a separate table
                    } else if (pi.PropertyType.IsClass) {
                        object objVal = pi.GetValue(container);
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(pi.PropertyType);
                        sb.Add(SetColumns(sqlHelper, tableName, subPropData, objVal, pi.PropertyType, prefix + prop.Name + "_", false));
                        sb.Add(",");
                    } else
                        throw new InternalError("Unknown property type {2} used in class {0}, property {1}", tpContainer.FullName, prop.Name, pi.PropertyType.FullName);
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
        /// Executes the provided PostgreSQL statement(s) and returns a scalar integer.
        /// </summary>
        /// <param name="sql">The PostgreSQL statement(s).</param>
        /// <returns>Returns a scalar integer.</returns>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to PostgreSQL repositories.</remarks>
        public async Task<int> Direct_ScalarIntAsync(string sql) {
            PostgreSQLHelper sqlHelper = new PostgreSQLHelper(Conn, null, Languages);
            string tableName = GetTableName();
            sql = sql.Replace("{TableName}", PostgreSQLBuilder.WrapQuotes(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $"[{SiteColumn}] = {SiteIdentity}");
            var o = await sqlHelper.ExecuteScalarAsync(sql);
            if (o == null || o.GetType() == typeof(System.DBNull))
                return 0;
            return Convert.ToInt32(o);
        }
        /// <summary>
        /// Executes the provided PostgreSQL statement(s).
        /// </summary>
        /// <param name="sql">The PostgreSQL statement(s).</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to PostgreSQL repositories.
        ///
        /// When using arguments, they are referenced in the PostgreSQL statement(s) <paramref name="sql"/> using @p1, @p2, etc. where @p1 is replaced by the first optional argument, etc.
        /// PostgreSQL injection attacks are not possible when using parameters.
        /// </remarks>
        public Task Direct_QueryAsync(string sql) {
            return Direct_QueryAsync(sql, new object[] { });
        }
        /// <summary>
        /// Executes the provided PostgreSQL statement(s).
        /// </summary>
        /// <param name="sql">The PostgreSQL statement(s).</param>
        /// <param name="args">Optional arguments that are passed when executing the PostgreSQL statements.</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to PostgreSQL repositories.
        ///
        /// When using arguments, they are referenced in the PostgreSQL statement(s) <paramref name="sql"/> using @p1, @p2, etc. where @p1 is replaced by the first optional argument.
        /// PostgreSQL injection attacks are not possible when using parameters.
        /// </remarks>
        public async Task Direct_QueryAsync(string sql, params object[] args) {
            PostgreSQLHelper sqlHelper = new PostgreSQLHelper(Conn, null, Languages);
            string tableName = GetTableName();
            int count = 0;
            foreach (object arg in args) {
                ++count;
                sqlHelper.AddParam($"p{count}", arg);
            }
            sql = sql.Replace("{TableName}", PostgreSQLBuilder.WrapQuotes(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $"[{SiteColumn}] = {SiteIdentity}");
            await sqlHelper.ExecuteNonQueryAsync(sql);
        }
        /// <summary>
        /// Executes the provided PostgreSQL statement(s) and returns a collection of objects (one for each row retrieved) of type {i}TYPE{/i}.
        /// </summary>
        /// <param name="sql">The PostgreSQL statement(s).</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to PostgreSQL repositories.</remarks>
        /// <returns>Returns a collection  of objects (one for each row retrieved) of type {i}TYPE{/i}.</returns>
        public async Task<List<TYPE>> Direct_QueryListAsync<TYPE>(string sql) {
            PostgreSQLHelper sqlHelper = new PostgreSQLHelper(Conn, null, Languages);
            string tableName = GetTableName();
            sql = sql.Replace("{TableName}", PostgreSQLBuilder.WrapQuotes(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $"[{SiteColumn}] = {SiteIdentity}");
            List<TYPE> list = new List<TYPE>();
            using (DbDataReader reader = await sqlHelper.ExecuteReaderAsync(sql)) {
                while (YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())
                    list.Add(sqlHelper.CreateObject<TYPE>(reader));
            }
            return list;
        }

        // ISQLTableInfo
        // ISQLTableInfo
        // ISQLTableInfo

        /// <summary>
        /// Returns the PostgreSQL connection string used by the data provider.
        /// </summary>
        /// <returns>Returns the PostgreSQL connection string used by the data provider.</returns>
        public string GetConnectionString() {
            return ConnectionString;
        }
        /// <summary>
        /// Returns the database name used by the data provider.
        /// </summary>
        /// <returns>Returns the PostgreSQL database name used by the data provider.</returns>
        public string GetDatabaseName() {
            return Database;
        }
        /// <summary>
        /// Returns the database owner used by the data provider.
        /// </summary>
        /// <returns>Returns the database owner used by the data provider.</returns>
        public string GetDbOwner() {
            return Schema;
        }
        /// <summary>
        /// Returns the table name used by the data provider.
        /// </summary>
        /// <returns>Returns the table name used by the data provider.</returns>
        public string GetTableName() {
            return PostgreSQLBuilder.BuildFullTableName(Database, Schema, Dataset);
        }
        /// <summary>
        /// Replaces search text in a PostgreSQL string fragment with the table name used by the data provider.
        /// </summary>
        /// <param name="text">A PostgreSQL fragment where occurrences of <paramref name="searchText"/> are replaced by the table name used by the data provider.</param>
        /// <param name="searchText">The text searched in <paramref name="text"/> that is replaced by the table name used by the data provider.</param>
        /// <returns>Returns the PostgreSQL string fragment with <paramref name="searchText"/> replaced by the table name used by the data provider.</returns>
        public string ReplaceWithTableName(string text, string searchText) {
            return text.Replace(searchText, GetTableName());
        }
        /// <summary>
        /// Replaces search text in a PostgreSQL string fragment with the language used by the data provider.
        /// </summary>
        /// <param name="text">A PostgreSQL fragment where occurrences of <paramref name="searchText"/> are replaced by the language used by the data provider.</param>
        /// <param name="searchText">The text searched in <paramref name="text"/> that is replaced by the language used by the data provider.</param>
        /// <returns>Returns the PostgreSQL string fragment with <paramref name="searchText"/> replaced by the language used by the data provider.</returns>
        public string ReplaceWithLanguage(string text, string searchText) {
            return text.Replace(searchText, GetLanguageSuffix());
        }
    }
}
