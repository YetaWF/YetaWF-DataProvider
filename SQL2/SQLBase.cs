/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading.Tasks;
using System.Transactions;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Language;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Support;
using YetaWF.Core.Support.Serializers;
using YetaWF.Core.Views.Shared;

namespace YetaWF.DataProvider.SQL2 {

    public abstract class SQLBase : IDisposable, IDataProviderTransactions {

        public static readonly string ExternalName = "SQL2";

        public const string DefaultString = "Default";
        public const string SQLConnectString = "SQLConnect";
        private const string SQLDboString = "SQLDbo";

        public const string SiteColumn = "__Site";
        public const string IdentityColumn = "Identity";
        public const string SubTableKeyColumn = "__Key";

        public Dictionary<string, object> Options { get; private set; }

        public Package Package { get; private set; }
        public string WebConfigArea { get; set; }
        public string Dataset { get; protected set; }
        public string Database { get; private set; }
        public string TypeName { get; protected set; }
        public int SiteIdentity { get; private set; }
        public int IdentitySeed { get; private set; }        
        public bool Cacheable { get; private set; }
        public bool Logging { get; private set; }
        public bool NoLanguages { get; private set; }
        public List<LanguageData> Languages { get; private set; }

        protected Func<string, string> CalculatedPropertyCallback { get; set; }

        public string ConnectionString { get; private set; }
        public string Dbo { get; private set; }
        public SqlConnection Conn { get; private set; }

        internal Database GetDatabase() {
            Server server = new Server(new ServerConnection(Conn));
            if (server.Databases == null || !server.Databases.Contains(Conn.Database))
                throw new InternalError("Can't connect to database {0}", Conn.Database);
            Database db = server.Databases[Conn.Database];
            return db;
        }

        public string AndSiteIdentity { get; private set; }

        private static object _lockObject = new object();

        protected SQLBase(Dictionary<string, object> options) {
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

            SqlConnectionStringBuilder sqlsb = new SqlConnectionStringBuilder(GetSqlConnectionString());
            sqlsb.MultipleActiveResultSets = true;
            ConnectionString = sqlsb.ToString();
            Dbo = GetSqlDbo();

            if (NoLanguages)
                Languages = new List<LanguageData>();
            else {
                Languages = MultiString.Languages;
                if (Languages.Count == 0) throw new InternalError("We need Languages");
            }
            if (SiteIdentity > 0)
                AndSiteIdentity = $"AND [{SiteColumn}] = {SiteIdentity}";

            Conn = new SqlConnection(ConnectionString);
            Conn.Open();///$$$ should move and make async
            Database = Conn.Database;

            DisposableTracker.AddObject(this);
        }
        public void Dispose() { Dispose(true); }
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

        protected string GetSqlConnectionString() {
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
        protected string GetSqlDbo() {
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

        protected bool HasIdentity(string identityName) {
            return !string.IsNullOrWhiteSpace(identityName);
        }

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

        public DataProviderTransaction StartTransaction() {
            if (Trans != null) throw new InternalError("StartTransaction has already been called for this data provider");
            Trans = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = IsolationLevel.Serializable });
            return new DataProviderTransaction(CommitTransaction, AbortTransaction);
        }
        public void CommitTransaction() {
            if (Trans == null) throw new InternalError("StartTransaction was not called for this data provider - nothing to commit");
            Trans.Complete();
            Trans.Dispose();
            Trans = null;
        }
        public void AbortTransaction() {
            if (Trans != null)
                Trans.Dispose();
            Trans = null;
        }

        // SORTS, FILTERS
        // SORTS, FILTERS
        // SORTS, FILTERS

        // Update column names for constructed names (as used in MultiString)
        protected List<DataProviderFilterInfo> NormalizeFilter(Type type, List<DataProviderFilterInfo> filters) {
            if (filters == null) return null;
            filters = (from f in filters select new DataProviderFilterInfo(f)).ToList();// copy list
            foreach (DataProviderFilterInfo f in filters)
                if (f.Field != null) f.Field = f.Field.Replace(".", "_");
            GridHelper.NormalizeFilters(type, filters);
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
        protected List<DataProviderSortInfo> NormalizeSort(Type type, List<DataProviderSortInfo> sorts) {
            if (sorts == null) return null;
            sorts = (from s in sorts select new DataProviderSortInfo(s)).ToList();// copy list
            foreach (DataProviderSortInfo sort in sorts) {
                PropertyInfo prop = ObjectSupport.GetProperty(type, sort.Field);
                if (prop.PropertyType == typeof(MultiString))
                    sort.Field = ColumnFromPropertyWithLanguage(MultiString.ActiveLanguage, sort.Field);
            }
            return sorts;
        }
        public static string ColumnFromPropertyWithLanguage(string langId, string field) {
            return field + "_" + langId.Replace("-", "_");
        }
        public static string GetLanguageSuffix() {
            return ColumnFromPropertyWithLanguage(MultiString.ActiveLanguage, "");
        }

        protected string MakeJoins(SQLHelper helper, List<JoinData> joins) {
            SQLBuilder sb = new SQLBuilder();
            if (joins != null) {
                foreach (JoinData join in joins) {
                    ISQLTableInfo joinInfo = (ISQLTableInfo)join.JoinDP.GetDataProvider();
                    string joinTable = joinInfo.GetTableName();
                    ISQLTableInfo mainInfo = (ISQLTableInfo)join.MainDP.GetDataProvider();
                    string mainTable = mainInfo.GetTableName();
                    if (join.JoinType == JoinData.JoinTypeEnum.Left)
                        sb.Add($"LEFT JOIN {joinTable}");
                    else
                        sb.Add($"INNER JOIN {joinTable}");
                    sb.Add(" ON ");
                    if (join.UseSite && SiteIdentity > 0)
                        sb.Add("(");
                    sb.Add($"{SQLBuilder.BuildFullColumnName(mainTable, join.MainColumn)} = {SQLBuilder.BuildFullColumnName(joinTable, join.JoinColumn)}");
                    if (join.UseSite && SiteIdentity > 0)
                        sb.Add($") AND {SQLBuilder.BuildFullColumnName(mainTable, SiteColumn)} = {SQLBuilder.BuildFullColumnName(joinTable, SiteColumn)}");
                }
            }
            return sb.ToString();
        }
        protected string MakeFilter(SQLHelper sqlHelper, List<DataProviderFilterInfo> filters, Dictionary<string, string> visibleColumns = null) {
            SQLBuilder sb = new SQLBuilder();
            if (filters != null && filters.Count() > 0) {
                if (SiteIdentity > 0)
                    sb.Add("WHERE (");
                else
                    sb.Add("WHERE ");
                sqlHelper.AddWhereExpr(sb, Dataset, filters, visibleColumns);
                if (SiteIdentity > 0)
                    sb.Add($") AND {SQLBuilder.BuildFullColumnName(Database, Dbo, Dataset, SiteColumn)} = {SiteIdentity}");
            } else {
                if (SiteIdentity > 0)
                    sb.Add($"WHERE {SQLBuilder.BuildFullColumnName(Database, Dbo, Dataset, SiteColumn)} = {SiteIdentity}");
            }
            return sb.ToString();
        }

        protected string MakeColumnList(SQLHelper sqlHelper, Dictionary<string, string> visibleColumns, List<JoinData> joins) {
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
        protected Dictionary<string, string> GetVisibleColumns(string databaseName, string dbOwner, string tableName, Type objType, List<JoinData> joins) {
            Dictionary<string, string> visibleColumns = new Dictionary<string, string>();
            tableName = tableName.Trim(new char[] { '[', ']' });
            List<string> columns = SQLCache.GetColumns(Conn, databaseName, tableName);
            AddVisibleColumns(visibleColumns, databaseName, dbOwner, tableName, columns);
            if (CalculatedPropertyCallback != null) {
                List<PropertyData> props = ObjectSupport.GetPropertyData(objType);
                props = (from p in props where p.CalculatedProperty select p).ToList();
                foreach (PropertyData prop in props)
                    visibleColumns.Add(prop.Name, prop.Name);
            }
            if (joins != null) {
                foreach (JoinData join in joins) {
                    ISQLTableInfo mainInfo = (ISQLTableInfo)join.MainDP.GetDataProvider();
                    databaseName = mainInfo.GetDatabaseName();
                    dbOwner = mainInfo.GetDbOwner();
                    tableName = mainInfo.GetTableName();
                    tableName = tableName.Split(new char[] { '.' }).Last().Trim(new char[] { '[', ']' });
                    columns = SQLCache.GetColumns(Conn, databaseName, tableName);
                    AddVisibleColumns(visibleColumns, databaseName, dbOwner, tableName, columns);
                    ISQLTableInfo joinInfo = (ISQLTableInfo)join.JoinDP.GetDataProvider();
                    databaseName = joinInfo.GetDatabaseName();
                    dbOwner = joinInfo.GetDbOwner();
                    tableName = joinInfo.GetTableName();
                    tableName = tableName.Split(new char[] { '.' }).Last().Trim(new char[] { '[', ']' });
                    columns = SQLCache.GetColumns(join.JoinDP.GetDataProvider().Conn, databaseName, tableName);
                    AddVisibleColumns(visibleColumns, databaseName, dbOwner, tableName, columns);
                }
            }
            return visibleColumns;
        }
        private void AddVisibleColumns(Dictionary<string, string> visibleColumns, string databaseName, string dbOwner, string tableName, List<string> columns) {
            foreach (string column in columns) {
                if (!visibleColumns.ContainsKey(column))
                    visibleColumns.Add(column, SQLBuilder.BuildFullColumnName(databaseName, dbOwner, tableName, column));
            }
        }

        protected string CalculatedProperties(Type objType) {
            if (CalculatedPropertyCallback == null) return null;
            SQLBuilder sb = new SQLBuilder();
            List<PropertyData> props = ObjectSupport.GetPropertyData(objType);
            props = (from p in props where p.CalculatedProperty select p).ToList();
            foreach (PropertyData prop in props) {
                string calcProp = CalculatedPropertyCallback(prop.Name);
                sb.Add($", ({calcProp}) AS [{prop.Name}]");
            }
            return sb.ToString();
        }
        protected string GetColumnList(List<PropertyData> propData, Type tpContainer,
                string prefix, bool topMost,
                bool SiteSpecific = false,
                bool WithDerivedInfo = false,
                bool SubTable = false) {
            SQLBuilder sb = new SQLBuilder();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        sb.Add($"[{prefix}{prop.Name}],");
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                        foreach (var lang in Languages)
                            sb.Add($"[{prefix}{ColumnFromPropertyWithLanguage(lang.Id, prop.Name)}],");
                    } else if (pi.PropertyType == typeof(Image)) {
                        sb.Add($"[{prefix}{prop.Name}],");
                    } else if (TryGetDataType(pi.PropertyType)) {
                        sb.Add($"[{prefix}{prop.Name}],");
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
        protected string GetValueList(SQLHelper sqlHelper, string tableName, object container, List<PropertyData> propData, Type tpContainer,
                string prefix = "", bool topMost = false,
                bool SiteSpecific = false,
                Type DerivedType = null, string DerivedTableName = null,
                bool SubTable = false) {
            SQLBuilder sb = new SQLBuilder();
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
                        BinaryFormatter binaryFmt = new BinaryFormatter { AssemblyFormat = FormatterAssemblyStyle.Simple };
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

        protected string SetColumns(SQLHelper sqlHelper, string tableName, List<PropertyData> propData, object container, Type tpContainer, string prefix = "", bool topMost = false, bool SiteSpecific = false) {
            SQLBuilder sb = new SQLBuilder();
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
                        BinaryFormatter binaryFmt = new BinaryFormatter { AssemblyFormat = FormatterAssemblyStyle.Simple };
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

        public async Task<int> Direct_ScalarIntAsync(string tableName, string sql) {
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            sql = sql.Replace("{TableName}", SQLBuilder.WrapBrackets(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $"[{SiteColumn}] = {SiteIdentity}");
            int val = Convert.ToInt32(await sqlHelper.ExecuteScalarAsync(sql));
            return val;
        }
        public async Task Direct_QueryAsync(string tableName, string sql) {
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            sql = sql.Replace("{TableName}", SQLBuilder.WrapBrackets(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $"[{SiteColumn}] = {SiteIdentity}");
            await sqlHelper.ExecuteNonQueryAsync(sql);
        }
        public async Task<List<TYPE>> Direct_QueryListAsync<TYPE>(string tableName, string sql) {
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            sql = sql.Replace("{TableName}", SQLBuilder.WrapBrackets(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $"[{SiteColumn}] = {SiteIdentity}");
            List<TYPE> list = new List<TYPE>();
            using (SqlDataReader reader = await sqlHelper.ExecuteReaderAsync(sql)) {
                while (YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())
                    list.Add(sqlHelper.CreateObject<TYPE>(reader));
            }
            return list;
        }

        public string GetConnectionString() {
            return ConnectionString;
        }

        public string GetDbOwner() {
            return Dbo;
        }

        public string GetTableName() {
            return SQLBuilder.BuildFullTableName(Database, Dbo, Dataset);
        }

        public string ReplaceWithTableName(string text, string searchText) {
            return text.Replace(searchText, GetTableName());
        }

        public string ReplaceWithLanguage(string text, string searchText) {
            return text.Replace(searchText, GetLanguageSuffix());
        }

        public string GetDatabaseName() {
            return Database;
        }
    }
}
