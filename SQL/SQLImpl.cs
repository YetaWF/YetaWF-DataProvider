/* Copyright © 2017 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using BigfootSQL;
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
using System.Text;
using System.Transactions;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Language;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Support;
using YetaWF.Core.Support.Serializers;
using YetaWF.Core.Views.Shared;

namespace YetaWF.DataProvider {

    public partial class SQLDataProviderImpl : IDisposable {

        public static readonly int IDENTITY_SEED = 1000;

        public SQLDataProviderImpl(string dbOwner, string connString, string tableName,
                bool Logging = true, bool NoLanguages = false, bool Cacheable = false,
                int CurrentSiteIdentity = 0, int IdentitySeed = 0, Func<string, string> CalculatedPropertyCallback = null) {
            DbOwner = dbOwner;
            SqlConnectionStringBuilder sqlsb = new SqlConnectionStringBuilder(connString);
            sqlsb.MultipleActiveResultSets = true;
            ConnString = sqlsb.ToString();
            this.TableName = tableName;
            this.Logging = Logging;
            this.Cacheable = Cacheable;
            this.IdentitySeed = IdentitySeed == 0 ? IDENTITY_SEED : IdentitySeed;
            this.CalculatedPropertyCallback = CalculatedPropertyCallback;

            this.CurrentSiteIdentity = CurrentSiteIdentity;

            if (NoLanguages)
                Languages = new List<LanguageData>();
            else {
                Languages = MultiString.Languages;
                if (Languages.Count == 0)
                    throw new InternalError("We need Languages");
            }
            Conn = new SqlConnection(ConnString);
            Conn.Open();
            this.DatabaseName = Conn.Database;

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
        //~SQLDataProviderImpl() { Dispose(false); }

        public SqlConnection Conn { get; set; }
        public string DbOwner { get; internal set; }
        // http://stackoverflow.com/questions/4439409/open-close-sqlconnection-or-keep-open
        protected string ConnString { get; set; }
        protected string DatabaseName { get; set; }
        public string TableName { get; private set; }
        protected bool Logging { get; set; }
        public bool UseIdentity { get; set; }
        public List<LanguageData> Languages { get; set; }
        public bool Cacheable { get; private set; }
        public int CurrentSiteIdentity { get; private set; }
        public int IdentitySeed { get; private set; }
        Func<string, string> CalculatedPropertyCallback { get; set; }

        public const string SiteColumn = "__Site";
        public const string SubTableKeyColumn = "__Key";

        private TransactionScope Trans { get; set; }

        public DataProviderTransaction StartTransaction() {
            if (Trans != null)
                throw new InternalError("StartTransaction has already been called for this data provider");
            Trans = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = IsolationLevel.Serializable });
            return new DataProviderTransaction(CommitTransaction, AbortTransaction);
        }
        public void CommitTransaction() {
            if (Trans == null)
                throw new InternalError("StartTransaction was not called for this data provider - nothing to commit");
            Trans.Complete();
            Trans.Dispose();
            Trans = null;
        }
        public void AbortTransaction() {
            if (Trans != null)
                Trans.Dispose();
            Trans = null;
        }

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

        internal Database GetDatabase() {
            Server server = new Server(new ServerConnection(Conn));
            if (server.Databases == null || !server.Databases.Contains(Conn.Database))
                throw new InternalError("Can't connect to database {0}", Conn.Database);
            Database db = server.Databases[Conn.Database];
            return db;
        }
        public string ReplaceWithLanguage(string text, string searchText) {
            return text.Replace(searchText, GetLanguageSuffix());
        }

        protected void AddCalculatedProperties(BigfootSQL.SqlHelper DB, Type objType) {
            if (CalculatedPropertyCallback == null) return;
            List<PropertyData> props = ObjectSupport.GetPropertyData(objType);
            props = (from p in props where p.CalculatedProperty select p).ToList();
            foreach (PropertyData prop in props)
                DB.Add(string.Format(", ({0}) AS [{1}]", CalculatedPropertyCallback(prop.Name), prop.Name));
        }
        public static string WrapBrackets(string token) {
            if (token.StartsWith("["))
                return token;
            else
                return "[" + token + "]";
        }
        protected string BuildFullTableName(string tableName) {
            return WrapBrackets(tableName);
        }
        private string BuildFullColumnName(string database, string dbOwner, string tableName, string siteColumn) {
            return WrapBrackets(database) + "." + WrapBrackets(dbOwner) + "." + BuildFullColumnName(tableName, siteColumn);
        }

        protected string BuildFullColumnName(string tableName, string column) {
            return BuildFullTableName(tableName) + "." + WrapBrackets(column);
        }

        public void DropAllTables() {
            // don't do any logging here - we might be deleting the tables needed for logging
            Database db = GetDatabase();
            int maxTimes = 5;
            for (int time = maxTimes ; time > 0 && db.Tables.Count > 0 ; --time) {
                List<Table> tables = (from Table t in db.Tables select t).ToList<Table>();
                foreach (Table table in tables) {
                    if (table.Schema == DbOwner) {
                        try {
                            table.Drop();
                        } catch (Exception) { }
                    }
                }
            }
            SqlCache.ClearCache();
        }
        protected bool DropTable(Database db, string tableName, List<string> errorList) {
            foreach (Table table in db.Tables) {
                if (table.Schema == DbOwner && table.Name == tableName) {
                    try {
                        table.Drop();
                        return true;
                    } catch (Exception exc) {
                        if (Logging) YetaWF.Core.Log.Logging.AddErrorLog("Couldn't drop table {0}", table.Name, exc);
                        errorList.Add(string.Format("Couldn't drop table {0}", table.Name));
                        while (exc != null && exc.Message != null) {
                            errorList.Add(exc.Message);
                            exc = exc.InnerException;
                        }
                        return false;
                    }
                }
            }
            errorList.Add(string.Format("Table {0} not found - can't be dropped", tableName));
            return false;
        }
        protected bool DropSubTables(Database db, string tableName, List<string> errorList) {
            bool status = true;
            string subtablePrefix = tableName + "_";
            Table[] tables = (from Table t in db.Tables select t).ToArray<Table>();
            foreach (Table table in tables) {
                if (table.Name.StartsWith(subtablePrefix))
                    if (!DropTable(db, table.Name, errorList))
                        status = false;
            }
            return status;
        }

        protected List<BigfootSQL.SqlHelper> subDBs = new List<BigfootSQL.SqlHelper>();

        protected string GetColumnList(BigfootSQL.SqlHelper DB, List<PropertyData> propData, Type tpContainer,
                string prefix, bool topMost,
                bool SiteSpecific = false,
                bool WithDerivedInfo = false,
                bool SubTable = false) {
            StringBuilder sb = new StringBuilder();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        sb.AppendFormat("[{0}{1}]", prefix, prop.Name);
                        sb.Append(",");
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                        foreach (var lang in Languages) {
                            sb.AppendFormat("[{0}{1}]", prefix, ColumnFromPropertyWithLanguage(lang.Id, prop.Name));
                            sb.Append(",");
                        }
                    } else if (pi.PropertyType == typeof(Image)) {
                        sb.AppendFormat("[{0}{1}]", prefix, prop.Name);
                        sb.Append(",");
                    } else if (TryGetDataType(pi.PropertyType)) {
                        sb.AppendFormat("[{0}{1}]", prefix, prop.Name);
                        sb.Append(",");
                    } else if (pi.PropertyType.IsClass /* && propmmd.Model != null*/ && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // This is a enumerated type, so we have to create separate values using this table's identity column as a link
                        ; // these values are added as a subtable
                    } else if (pi.PropertyType.IsClass /*&& propmmd.Model != null*/) {
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(pi.PropertyType);
                        string columns = GetColumnList(DB, subPropData, pi.PropertyType, prefix + prop.Name + "_", false, SiteSpecific: false);
                        if (columns.Length > 0) {
                            sb.Append(columns);
                            sb.Append(",");
                        }
                    } else
                        throw new InternalError("Unknown property type {2} used in class {0}, property {1}", tpContainer.FullName, prop.Name, pi.PropertyType.FullName);
                }
            }
            if (SiteSpecific) {
                sb.AppendFormat("[{0}{1}]", prefix, SiteColumn);
                sb.Append(",");
            }
            if (WithDerivedInfo) {
                sb.AppendFormat("[{0}{1}]", prefix, "DerivedDataTableName");
                sb.Append(",");
                sb.AppendFormat("[{0}{1}]", prefix, "DerivedDataType");
                sb.Append(",");
                sb.AppendFormat("[{0}{1}]", prefix, "DerivedAssemblyName");
                sb.Append(",");
            }
            if (SubTable) {
                sb.AppendFormat("[{0}{1}]", prefix, SubTableKeyColumn);
                sb.Append(",");
            }
            if (sb.Length > 0)
                sb.Remove(sb.Length - 1, 1);// remove last ,
            return sb.ToString();
        }
        protected string GetValueList(BigfootSQL.SqlHelper DB, string tableName, List<PropertyData> propData, object container, Type tpContainer,
                string prefix="", bool topMost=false,
                bool SiteSpecific = false,
                Type DerivedType = null, string DerivedTableName = null,
                bool SubTable = false) {
            StringBuilder sb = new StringBuilder();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        object val = pi.GetValue(container);
                        if (val != null) {
                            if (pi.PropertyType == typeof(byte[])) {
                                sb.Append(DB.AddTempParam(val));
                            } else {
                                byte[] data = new GeneralFormatter().Serialize(val);
                                sb.Append(DB.AddTempParam(data));
                            }
                        } else {
                            sb.Append(DB.AddNullTempParam());
                        }
                        sb.Append(",");
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                        MultiString ms = (MultiString) pi.GetValue(container);
                        foreach (var lang in Languages) {
                            sb.Append(DB.AddTempParam(ms[lang.Id] ?? ""));
                            sb.Append(",");
                        }
                    } else if (pi.PropertyType == typeof(Image)) {
                        object val = pi.GetValue(container);
                        BinaryFormatter binaryFmt = new BinaryFormatter { AssemblyFormat = FormatterAssemblyStyle.Simple };
                        using (MemoryStream ms = new MemoryStream()) {
                            binaryFmt.Serialize(ms, val);
                            sb.Append(DB.AddTempParam(ms.ToArray()));
                        }
                        sb.Append(",");
                    } else if (pi.PropertyType == typeof(TimeSpan)) {
                        TimeSpan val = (TimeSpan) pi.GetValue(container);
                        long ticks = val.Ticks;
                        sb.Append(DB.AddTempParam(ticks));
                        sb.Append(",");
                    } else if (TryGetDataType(pi.PropertyType)) {
                        sb.Append(DB.AddTempParam(pi.GetValue(container)));
                        sb.Append(",");
                    } else if (pi.PropertyType.IsClass /* && propmmd.Model != null*/ && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // This is a enumerated type, so we have to create separate values using this table's identity column as a link

                        // determine the enumerated type
                        Type subType = pi.PropertyType.GetInterfaces().Where(t => t.IsGenericType == true && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                                .Select(t => t.GetGenericArguments()[0]).FirstOrDefault();
                        // create an insert statement for the subtable
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subType);
                        string subTableName = tableName + "_" + pi.Name;
                        IEnumerable ienum = (IEnumerable)pi.GetValue(container);
                        foreach (var obj in ienum) {
                            BigfootSQL.SqlHelper subDB = new BigfootSQL.SqlHelper(DB.SqlConnection, DB.SqlTransaction, Languages);
                            subDB.INSERTINTO(subTableName, GetColumnList(subDB, subPropData, subType, "", true, SiteSpecific: false, SubTable: true))
                                    .VALUES(GetValueList(subDB, subTableName, subPropData, obj, subType, SiteSpecific: false, SubTable: true));
                            subDBs.Add(subDB);
                        }
                    } else if (pi.PropertyType.IsClass) {
                        object objVal = pi.GetValue(container);
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(pi.PropertyType);
                        string values = GetValueList(DB, tableName, subPropData, objVal, pi.PropertyType, prefix + prop.Name + "_", false);
                        if (values.Length > 0) {
                            sb.Append(values);
                            sb.Append(",");
                        }
                    } else
                        throw new InternalError("Unknown property type {2} used in class {0}, property {1}", tpContainer.FullName, prop.Name, prop.PropInfo.PropertyType.FullName);
                }
            }
            if (SiteSpecific) {
                sb.Append(DB.AddTempParam(CurrentSiteIdentity));
                sb.Append(",");
            }
            if (DerivedType != null) {
                if (DerivedTableName == null) throw new InternalError("Missing DerivedTableName");
                sb.Append(DB.AddTempParam(DerivedTableName));
                sb.Append(",");
                sb.Append(DB.AddTempParam(DerivedType.FullName));
                sb.Append(",");
                sb.Append(DB.AddTempParam(DerivedType.Assembly.FullName.Split(new char[]{','}, 2).First()));
                sb.Append(",");
            }
            if (SubTable) {
                sb.Append("@__Identity,");
            }
            if (sb.Length > 0)
                sb.Remove(sb.Length - 1, 1);// remove last ,
            return sb.ToString();
        }

        protected void ReadSubTables(BigfootSQL.SqlHelper DB, string tableName, string identityName, List<PropertyData> propData, object container, Type tpContainer) {
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        ; // nothing
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        ; // nothing
                    } else if (pi.PropertyType == typeof(Image)) {
                        ; // nothing
                    } else if (pi.PropertyType == typeof(TimeSpan)) {
                        ; // nothing
                    } else if (TryGetDataType(pi.PropertyType)) {
                        ; // nothing
                    } else if (pi.PropertyType.IsClass && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // This is a enumerated type, so we have to read separate values using this table's identity column as a link
                        object subContainer = pi.GetValue(container);
                        if (subContainer == null) throw new InternalError("ReadSubTables encountered a enumeration property that is null");
                        // determine the enumerated type
                        Type subType = pi.PropertyType.GetInterfaces().Where(t => t.IsGenericType == true && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                                .Select(t => t.GetGenericArguments()[0]).FirstOrDefault();
                        // get the identity value
                        PropertyInfo piIdent = ObjectSupport.TryGetProperty(tpContainer, identityName);
                        if (piIdent == null) throw new InternalError("Can't determine identity value");
                        int identityValue = (int) piIdent.GetValue(container);

                        // find the Add method for the collection so we can add each item as its read
                        MethodInfo addMethod = pi.PropertyType.GetMethod("Add", new Type[] { subType });
                        if (addMethod == null) throw new InternalError("ReadSubTables encountered a enumeration property that doesn't have an Add method");

                        // create a select statement for the subtable
                        string subTableName = tableName + "_" + pi.Name;
                        BigfootSQL.SqlHelper subDB = new BigfootSQL.SqlHelper(DB.SqlConnection, DB.SqlTransaction, Languages);
                        subDB.SELECT_ALL_FROM(subTableName).WHERE(SubTableKeyColumn, identityValue);

                        // read each record and add it to the collection
                        ObjectHelper objHelper = new ObjectHelper(Languages);
                        SqlDataReader rdr = subDB.ExecuteReader();
                        while (rdr.Read()) {
                            object obj = Activator.CreateInstance(subType);
                            objHelper.FillObject(rdr, obj);
                            addMethod.Invoke(subContainer, new object[] { obj });
                        }
                        rdr.Close();
                    }
                }
            }
        }
        //protected void JoinSubTables(BigfootSQL.SqlHelper DB, string tableName, string identityName, List<PropertyData> propData, Type tpContainer) {
        //    foreach (PropertyData prop in propData) {
        //        PropertyInfo pi = prop.PropInfo;
        //        if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty) {
        //            if (prop.HasAttribute(Data_Identity.AttributeName)) {
        //                ; // nothing
        //            } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
        //                ; // nothing
        //            } else if (pi.PropertyType == typeof(MultiString)) {
        //                ; // nothing
        //            } else if (pi.PropertyType == typeof(Image)) {
        //                ; // nothing
        //            } else if (pi.PropertyType == typeof(TimeSpan)) {
        //                ; // nothing
        //            } else if (TryGetDataType(pi.PropertyType)) {
        //                ; // nothing
        //            } else if (pi.PropertyType.IsClass && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
        //                // This is a enumerated type, so we have to read separate values using this table's identity column as a link
        //                string subTableName = tableName + "_" + pi.Name;
        //                DB.LEFTJOIN(subTableName).ON("["+tableName + "].[" + identityName + "]", "["+subTableName + "].[" + SubTableKeyColumn + "]");
        //            }
        //        }
        //    }
        //}
        protected void AddSetColumns(BigfootSQL.SqlHelper DB, string tableName, string identityName, List<PropertyData> propData, object container, Type tpContainer, string prefix = "", bool topMost = false, bool SiteSpecific = false) {
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        object val = pi.GetValue(container);
                        if (pi.PropertyType == typeof(byte[])) {
                            DB.SET(prefix + prop.Name, val);
                        } else if (val == null) {
                            DB.SET(prefix + prop.Name, null);
                        } else {
                            byte[] data = new GeneralFormatter().Serialize(val);
                            DB.SET(prefix + prop.Name, data);
                        }
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                        MultiString ms = (MultiString) pi.GetValue(container);
                        foreach (var lang in Languages) {
                            DB.SET(prefix + ColumnFromPropertyWithLanguage(lang.Id, prop.Name), ms[lang.Id]);
                        }
                    } else if (pi.PropertyType == typeof(Image)) {
                        object val = pi.GetValue(container);
                        BinaryFormatter binaryFmt = new BinaryFormatter { AssemblyFormat = FormatterAssemblyStyle.Simple };
                        using (MemoryStream ms = new MemoryStream()) {
                            binaryFmt.Serialize(ms, val);
                            DB.SET(prefix + prop.Name, ms.ToArray());
                        }
                    } else if (pi.PropertyType == typeof(TimeSpan)) {
                        TimeSpan val = (TimeSpan) pi.GetValue(container);
                        long ticks = val.Ticks;
                        DB.SET(prefix + prop.Name, ticks);
                    } else if (TryGetDataType(pi.PropertyType)) {
                        DB.SET(prefix + prop.Name, pi.GetValue(container));
                    } else if (pi.PropertyType.IsClass && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // This is a enumerated type, so we have to create a separate values using this table's identity column as a link
                        // determine the enumerated type
                        Type subType = pi.PropertyType.GetInterfaces().Where(t => t.IsGenericType == true && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                                .Select(t => t.GetGenericArguments()[0]).FirstOrDefault();
                        // get the identity value
                        PropertyInfo piIdent = ObjectSupport.TryGetProperty(tpContainer, identityName);
                        if (piIdent == null) throw new InternalError("Can't determine identity value");
                        int identityValue = (int) piIdent.GetValue(container);
                        // create an insert statement for the subtable
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subType);
                        string subTableName = tableName + "_" + pi.Name;
                        IEnumerable ienum = (IEnumerable) pi.GetValue(container);;
                        BigfootSQL.SqlHelper subDB = new BigfootSQL.SqlHelper(DB.SqlConnection, DB.SqlTransaction, Languages);
                        // delete all existing entries from subtable
                        subDB.DELETEFROM(subTableName).WHERE(SubTableKeyColumn, identityValue).ExecuteNonquery();
                        // add new entries
                        foreach (var obj in ienum) {
                            subDB.INSERTINTO(subTableName, GetColumnList(subDB, subPropData, subType, "", true, SiteSpecific: false, SubTable: true))
                                    .VALUES(GetValueList(subDB, subTableName, subPropData, obj, subType, SiteSpecific: false, SubTable: true));
                        }
                        subDB.AddParam("__Identity", identityValue);
                        subDB.ExecuteNonquery();
                    } else if (pi.PropertyType.IsClass) {
                        object objVal = pi.GetValue(container);
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(pi.PropertyType);
                        AddSetColumns(DB, tableName, identityName, subPropData, objVal, pi.PropertyType, prefix + prop.Name + "_", false);
                    } else
                        throw new InternalError("Unknown property type {2} used in class {0}, property {1}", tpContainer.FullName, prop.Name, pi.PropertyType.FullName);
                }
            }
            if (SiteSpecific) {
                DB.SET(prefix + SiteColumn, CurrentSiteIdentity);
            }
        }
        // Flatten the current table(with joins) and create a lookup table for all fields.
        // If a joined table has a field with the same name as the lookup table, it is not accessible.
        protected Dictionary<string, string> GetVisibleColumns(string databaseName, string dbOwner, string tableName, Type objType, List<JoinData> joins) {
            Dictionary<string, string> visibleColumns = new Dictionary<string, string>();
            tableName = tableName.Trim(new char[] { '[', ']' });
            List<string> columns = SqlCache.GetColumns(Conn, databaseName, tableName);
            AddVisibleColumns(visibleColumns, databaseName, dbOwner, tableName, columns);
            if (CalculatedPropertyCallback != null) {
                List<PropertyData> props = ObjectSupport.GetPropertyData(objType);
                props = (from p in props where p.CalculatedProperty select p).ToList();
                foreach (PropertyData prop in props) {
                    visibleColumns.Add(prop.Name, prop.Name);
                }
            }
            if (joins != null) {
                foreach (JoinData join in joins) {
                    databaseName = join.MainDP.GetDatabaseName();
                    dbOwner = join.MainDP.GetDbOwner();
                    tableName = join.MainDP.GetTableName();
                    tableName = tableName.Split(new char[] { '.' }).Last().Trim(new char[] { '[', ']' });
                    columns = SqlCache.GetColumns(Conn, databaseName, tableName);
                    AddVisibleColumns(visibleColumns, databaseName, dbOwner, tableName, columns);
                    databaseName = join.JoinDP.GetDatabaseName();
                    dbOwner = join.JoinDP.GetDbOwner();
                    tableName = join.JoinDP.GetTableName();
                    tableName = tableName.Split(new char[] { '.' }).Last().Trim(new char[] { '[', ']' });
                    columns = SqlCache.GetColumns(Conn, databaseName, tableName);
                    AddVisibleColumns(visibleColumns, databaseName, dbOwner, tableName, columns);
                }
            }
            return visibleColumns;
        }
        private void AddVisibleColumns(Dictionary<string, string> visibleColumns, string databaseName, string dbOwner, string tableName, List<string> columns) {
            foreach (string column in columns) {
                if (!visibleColumns.ContainsKey(column))
                    visibleColumns.Add(column, BuildFullColumnName(databaseName, dbOwner, tableName, column));
            }
        }

        protected void MakeJoins(SqlHelper DB, List<JoinData> joins) {
            if (joins != null) {
                foreach (JoinData join in joins) {
                    string joinTable = join.JoinDP.GetTableName();
                    string mainTable = join.MainDP.GetTableName();
                    if (join.JoinType == JoinData.JoinTypeEnum.Left)
                        DB.LEFTJOIN(joinTable);
                    else
                        DB.INNERJOIN(joinTable);
                    DB.ON();
                    if (join.UseSite && CurrentSiteIdentity > 0)
                        DB.OP();
                    DB.Add(BuildFullColumnName(mainTable, join.MainColumn));
                    DB.Add("=");
                    DB.Add(BuildFullColumnName(joinTable, join.JoinColumn));
                    if (join.UseSite && CurrentSiteIdentity > 0) {
                        DB.CP();
                        DB.AND();
                        DB.Add(BuildFullColumnName(mainTable, SiteColumn));
                        DB.Add("=");
                        DB.Add(BuildFullColumnName(joinTable, SiteColumn));
                    }
                }
            }
        }

        protected void MakeFilter(BigfootSQL.SqlHelper DB, List<DataProviderFilterInfo> filters, Dictionary<string,string> visibleColumns = null) {
            if (filters != null && filters.Count() > 0) {
                if (CurrentSiteIdentity > 0) {
                    DB.WHERE().OP();
                } else {
                    DB.WHERE();
                }
                DB.WHERE_EXPR(TableName, filters, visibleColumns);
                if (CurrentSiteIdentity > 0) {
                    DB.CP().AND(BuildFullColumnName(DatabaseName, DbOwner, TableName, SiteColumn), CurrentSiteIdentity);
                }
            } else {
                if (CurrentSiteIdentity > 0) {
                    DB.WHERE(BuildFullColumnName(DatabaseName, DbOwner, TableName, SiteColumn), CurrentSiteIdentity);
                }
            }
        }

        private static string ColumnFromPropertyWithLanguage(string langId, string field) {
            return field + "_" + langId.Replace("-", "_");
        }
        public static string GetLanguageSuffix() {
            return ColumnFromPropertyWithLanguage(MultiString.ActiveLanguage, "");
        }

        // Update column names for constructed names (as used in MultiString)
        protected List<DataProviderFilterInfo> NormalizeFilter(Type type, List<DataProviderFilterInfo> filters) {
            if (filters == null) return null;
            filters = (from f in filters select new DataProviderFilterInfo(f)).ToList();// copy list
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

        public int Direct_ScalarInt(string tableName, string sql) {
            BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(Conn, Languages);
            sql = sql.Replace("{TableName}", SQLDataProviderImpl.WrapBrackets(tableName));
            if (CurrentSiteIdentity > 0)
                sql = sql.Replace("{__Site}", "[__Site] = " + CurrentSiteIdentity.ToString());
            DB.RawBuilder.Append(sql);
            int val = DB.ExecuteScalarInt();
            return val;
        }
        public void Direct_Query(string tableName, string sql) {
            BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(Conn, Languages);
            sql = sql.Replace("{TableName}", SQLDataProviderImpl.WrapBrackets(tableName));
            if (CurrentSiteIdentity > 0)
                sql = sql.Replace("{__Site}", "[__Site] = " + CurrentSiteIdentity.ToString());
            DB.RawBuilder.Append(sql);
            DB.ExecuteNonquery();
        }
        public int Direct_QueryRetVal(string tableName, string sql) {
            BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(Conn, Languages);
            sql = sql.Replace("{TableName}", SQLDataProviderImpl.WrapBrackets(tableName));
            if (CurrentSiteIdentity > 0)
                sql = sql.Replace("{__Site}", "[__Site] = " + CurrentSiteIdentity.ToString());
            DB.RawBuilder.Append(sql);
            return DB.ExecuteQueryRetVal();
        }
        public List<TYPE> Direct_QueryList<TYPE>(string tableName, string sql) {
            BigfootSQL.SqlHelper DB = new BigfootSQL.SqlHelper(Conn, Languages);
            sql = sql.Replace("{TableName}", SQLDataProviderImpl.WrapBrackets(tableName));
            if (CurrentSiteIdentity > 0)
                sql = sql.Replace("{__Site}", "[__Site] = " + CurrentSiteIdentity.ToString());
            DB.RawBuilder.Append(sql);
            return DB.ExecuteCollection<TYPE>();
        }

        // Index removal (for upgrades only)

        private static List<string> DBsCompleted;

        protected void RemoveIndexesIfNeeded(Database db) {
            if (!Package.MajorDataChange) return;
            if (DBsCompleted == null) DBsCompleted = new List<string>();
            if (DBsCompleted.Contains(db.Name)) return; // already done
            // do multiple passes until no more indexes available (we don't want to figure out the dependencies)
            int passes = 0;
            for ( ; ; ++passes) {
                int drop = 0;
                int failures = 0;
                foreach (Table table in db.Tables) {
                    int tableFailures = 0;
                    for (int i = table.ForeignKeys.Count; i > 0; --i) {
                        try {
                            table.ForeignKeys[i - 1].Drop();
                            ++drop;
                        } catch (Exception) { ++tableFailures; }
                    }
                    for (int i = table.Indexes.Count; i > 0; --i) {
                        try {
                            table.Indexes[i - 1].Drop();
                            ++drop;
                        } catch (Exception) { ++tableFailures; }
                    }
                    if (tableFailures == 0) {
                        foreach (Column column in table.Columns) {
                            if (column.DefaultConstraint != null)
                                column.DefaultConstraint.Drop();
                        }
                        table.Alter();
                    }
                    failures += tableFailures;
                }
                if (failures == 0)
                    break;// successfully removed everything
                if (drop == 0) {
                    throw new InternalError("No index/foreign keys could be dropped on the last pass in DB {0}", db.Name);
                }
            }
            DBsCompleted.Add(db.Name);
        }
    }
}