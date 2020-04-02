/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Npgsql;
using Npgsql.NameTranslation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Transactions;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Packages;
using YetaWF.Core.Support;
using YetaWF.DataProvider.SQLGeneric;

namespace YetaWF.DataProvider.PostgreSQL {

    /// <summary>
    /// This abstract class is the base class for all PostgreSQL low-level data providers.
    /// </summary>
    public abstract class SQLBase : SQLGenericBase, IDataProviderTransactions {

        /// <summary>
        /// Defines the name of the PostgreSQL low-level data provider.
        /// This name is used in AppSettings.json ("IOMode": "PostgreSQL").
        /// </summary>
        public const string ExternalName = "PostgreSQL";
        /// <summary>
        /// Defines the key used in appsettings.json to define a PostgreSQL connection string.
        /// ("PostgreSQLConnect": "Host=..host..;Port=..port..;Database=..database..;User ID=..userid..;Password=..password..").
        /// </summary>
        public const string PostgreSQLConnectString = "PostgreSQLConnect";
        /// <summary>
        /// Defines the key used in appsettings.json to define a PostgreSQL schema.
        /// </summary>
        public const string PostgreSQLSchemaString = "PostgreSQLSchema";

        /// <summary>
        /// Defines the PostgreSQL connection string used by this data provider.
        /// </summary>
        /// <remarks>The connection string is defined in appsettings.json but may be modified by the data provider.
        /// The ConnectionString property contains the actual connection string used to connect to the PostgreSQL database.
        /// </remarks>
        public string ConnectionString { get; private set; }
        /// <summary>
        /// Defines the database schema used by this data provider.
        /// </summary>
        /// <remarks>The database schema is defined in AppSettings.json ("PostgreSQLSchema": "public").
        /// </remarks>
        public string Schema { get; private set; }
        /// <summary>
        /// The underlying Nqgsql.NpgsqlConnection object used to connect to the database.
        /// </summary>
        public NpgsqlConnection Conn { get; private set; }
        private bool ConnDependent { get; set; }

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
            NpgsqlConnectionStringBuilder sqlsb = new NpgsqlConnectionStringBuilder(GetPostgreSqlConnectionString());
            // sqlsb....
            ConnectionString = sqlsb.ToString();
            Schema = GetPostgreSqlSchema();

            Conn = new NpgsqlConnection(ConnectionString);
            ConnDependent = false;
        }
        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="disposing">true to close the database connection and release the DisposableTracker reference count, false otherwise.</param>
        protected override void Dispose(bool disposing) {
            base.Dispose(disposing);
            if (disposing) {
                if (Conn != null) {
                    if (!ConnDependent) {
                        Conn.Close();
                        Conn.Dispose();
                    }
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
                    Conn.OpenAsync().Wait();
                }
            }
            Database = Conn.Database;
            return Task.CompletedTask;
        }
        private static readonly object OpenLock = new object();

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

        internal void AddCompositeMapping(Type type, string pgType) {
            lock (lockObject) {
                if (TypeList.Contains(pgType)) return;
                Conn.TypeMapper.MapComposite(type, pgType, Translator);
                TypeList.Add(pgType);
            }
        }
        internal static NpgsqlNullNameTranslator Translator => new NpgsqlNullNameTranslator();
        private List<string> TypeList => new List<string>();// keep track of mapped types
        private static object lockObject => new object();

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
            if (!ConnDependent) {
                Conn.Dispose();
                Conn.Close();
                Conn = null;
            }
            // release all dependent dataprovider's connection
            foreach (DataProviderImpl dp in dps) {
                SQLBase sqlBase = (SQLBase)dp.GetDataProvider();
                if (!sqlBase.ConnDependent) {
                    sqlBase.Conn.Close();
                    sqlBase.Conn.Dispose();
                }
                sqlBase.Conn = null;
            }

            // Make a new connection
            ownerSqlBase.Conn = new NpgsqlConnection(ownerSqlBase.ConnectionString);
            ownerSqlBase.ConnDependent = false;

            // all dependent data providers have to use the same connection
            foreach (DataProviderImpl dp in dps) {
                SQLBase sqlBase = (SQLBase)dp.GetDataProvider();
                sqlBase.Conn = ownerSqlBase.Conn;
                sqlBase.ConnDependent = true;
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

        internal string MakeFilter(SQLHelper sqlHelper, List<DataProviderFilterInfo> filters, Dictionary<string, string> visibleColumns = null) {
            SQLBuilder sb = new SQLBuilder();
            if (filters != null && filters.Count() > 0) {
                if (SiteIdentity > 0)
                    sb.Add("WHERE (");
                else
                    sb.Add("WHERE ");
                sqlHelper.AddWhereExpr(sb, Dataset, filters, visibleColumns);
                if (SiteIdentity > 0)
                    sb.Add($") AND {sb.BuildFullColumnName(Database, Schema, Dataset, SiteColumn)} = {SiteIdentity}");
            } else {
                if (SiteIdentity > 0)
                    sb.Add($"WHERE {sb.BuildFullColumnName(Database, Schema, Dataset, SiteColumn)} = {SiteIdentity}");
            }
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
            await EnsureOpenAsync();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            string tableName = GetTableName();
            sql = sql.Replace("{TableName}", SQLBuilder.WrapIdentifier(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $@"""{SiteColumn}"" = {SiteIdentity}");
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
        /// Use of this method limits the application data provider to PostgreSQL repositories.
        ///
        /// When using arguments, they are referenced in the SQL statement(s) <paramref name="sql"/> using @p1, @p2, etc. where @p1 is replaced by the first optional argument.
        /// SQL injection attacks are not possible when using parameters.
        /// </remarks>
        public async Task Direct_QueryAsync(string sql) {
            await EnsureOpenAsync();
            await Direct_QueryAsync(sql, Array.Empty<object>());
        }
        /// <summary>
        /// Executes the provided PostgreSQL statement(s).
        /// </summary>
        /// <param name="sql">The SQL statement(s).</param>
        /// <param name="args">Optional arguments that are passed when executing the SQL statements.</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to PostgreSQL repositories.
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
                sqlHelper.AddParam($"@p{count}", arg);
            }
            sql = sql.Replace("{TableName}", SQLBuilder.WrapIdentifier(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $@"""{SiteColumn}"" = {SiteIdentity}");
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
        /// Use of this method limits the application data provider to PostgreSQL repositories.</remarks>
        /// <returns>Returns an object of type {i}TYPE{/i}.</returns>
        public async Task<TYPE> Direct_QueryAsync<TYPE>(string sql, params object[] args) {
            await EnsureOpenAsync();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
            string tableName = GetTableName();
            int count = 0;
            foreach (object arg in args) {
                ++count;
                sqlHelper.AddParam($"@p{count}", arg);
            }
            sql = sql.Replace("{TableName}", SQLBuilder.WrapIdentifier(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($@"{{{SiteColumn}}}", $@"""{SiteColumn}"" = {SiteIdentity}");
            List<TYPE> list = new List<TYPE>();
            using (NpgsqlDataReader reader = await sqlHelper.ExecuteReaderAsync(sql)) {
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
                sqlHelper.AddParam($"@p{count}", arg);
            }
            sql = sql.Replace("{TableName}", SQLBuilder.WrapIdentifier(tableName));
            if (SiteIdentity > 0)
                sql = sql.Replace($"{{{SiteColumn}}}", $@"""{SiteColumn}"" = {SiteIdentity}");
            List<TYPE> list = new List<TYPE>();
            using (NpgsqlDataReader reader = await sqlHelper.ExecuteReaderAsync(sql)) {
                while (YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())
                    list.Add(sqlHelper.CreateObject<TYPE>(reader));
            }
            return list;
        }

        /// <summary>
        /// Executes the provided SQL statement(s) and returns a paged collection of objects (one for each row retrieved) of type {i}TYPE{/i}.
        /// </summary>
        /// <param name="sqlCount">The SQL statement(s) to return the total number of records (not paged).</param>
        /// <param name="sql">The SQL statement(s) to return a collection of objects of type {i}TYPE{/i}.</param>
        /// <param name="skip">The number of records to skip (paging support).</param>
        /// <param name="take">The number of records to retrieve (paging support). If more records are available they are dropped.</param>
        /// <param name="sort">A collection describing the sort order.</param>
        /// <param name="filters">A collection describing the filtering criteria.</param>
        /// <param name="args">Optional arguments that are passed when executing the SQL statements.</param>
        /// <remarks>This is used by application data providers to build and execute complex queries that are not possible with the standard data providers.
        /// Use of this method limits the application data provider to PostgreSQL repositories.</remarks>
        /// <returns>Returns a collection  of objects (one for each row retrieved) of type {i}TYPE{/i}.</returns>
        /// <remarks>
        /// $WhereFilter$ and $OrderBy$ embedded in the SQL statements are replaced with a complete WHERE clause for filtering and the column names for sorting, respectively.
        ///
        /// Two SQL statements must be provided, one to return a scalar value with the total number of records (not paged) and the second to return a collection of objects of type {i}TYPE{/i}.
        /// </remarks>
        public async Task<DataProviderGetRecords<TYPE>> Direct_QueryPagedListAsync<TYPE>(string sqlCount, string sql, int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, params object[] args) {

            await EnsureOpenAsync();

            string tableName = GetTableName();

            // get count
            if (skip != 0 || take != 0) {
                SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                int count = 0;
                foreach (object arg in args) {
                    ++count;
                    sqlHelper.AddParam($"@p{count}", arg);
                }

                sqlCount = sqlCount.Replace("{TableName}", SQLBuilder.WrapIdentifier(tableName));
                if (SiteIdentity > 0)
                    sqlCount = sqlCount.Replace($"{{{SiteColumn}}}", $@"""{SiteColumn}"" = {SiteIdentity}");

                filters = NormalizeFilter(typeof(TYPE), filters);
                string filter = MakeFilter(sqlHelper, filters, null);
                sqlCount = sqlCount.Replace("$WhereFilter$", filter);
                sqlCount += "\n\n" + sqlHelper.DebugInfo;

                int total;
                using (NpgsqlDataReader reader = await sqlHelper.ExecuteReaderAsync(sqlCount)) {
                    if (!(YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) throw new InternalError("Expected # of records");
                    total = reader.GetInt32(0);
                }
                if (total == 0)
                    return new DataProviderGetRecords<TYPE>();
            }

            // get records 
            {
                SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);
                SQLBuilder sb = new SQLBuilder();

                int count = 0;
                foreach (object arg in args) {
                    ++count;
                    sqlHelper.AddParam($"@p{count}", arg);
                }

                sql = sql.Replace("{TableName}", SQLBuilder.WrapIdentifier(tableName));
                if (SiteIdentity > 0)
                    sql = sql.Replace($"{{{SiteColumn}}}", $@"""{SiteColumn}"" = {SiteIdentity}");

                sort = NormalizeSort(typeof(TYPE), sort);
                sql = sql.Replace("$OrderBy$", sb.GetOrderBy(null, sort, Offset: skip, Next: take));

                filters = NormalizeFilter(typeof(TYPE), filters);
                string filter = MakeFilter(sqlHelper, filters, null);
                sql = sql.Replace("$WhereFilter$", filter);
                sql += "\n\n" + sqlHelper.DebugInfo;

                DataProviderGetRecords<TYPE> recs = new DataProviderGetRecords<TYPE>();

                using (NpgsqlDataReader reader = await sqlHelper.ExecuteReaderAsync(sql)) {
                    while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) {
                        TYPE o = sqlHelper.CreateObject<TYPE>(reader);
                        recs.Data.Add(o);
                    }
                }
                return recs;
            }
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
            using (NpgsqlDataReader reader = await sqlHelper.ExecuteReaderStoredProcAsync(sqlProc)) {

                while ((YetaWFManager.IsSync() ? reader.Read() : await reader.ReadAsync())) {
                    TYPE o = sqlHelper.CreateObject<TYPE>(reader);
                    recs.Data.Add(o);
                }
                recs.Total = recs.Data.Count;
            }
            return recs;
        }

        // IPostgreSQLTableInfo
        // IPostgreSQLTableInfo
        // IPostgreSQLTableInfo

        /// <summary>
        /// Returns an IPostgreSQLTableInfo interface for the data provider.
        /// </summary>
        /// <returns>Returns an IPostgreSQLTableInfo interface for the data provider.</returns>
        public async Task<IPostgreSQLTableInfo> GetIPostgreSQLTableInfoAsync() {
            await EnsureOpenAsync();
            return (IPostgreSQLTableInfo)this;
        }

        /// <summary>
        /// Returns the PostgreSQL connection string used by the data provider.
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
        public string GetSchema() {
            return Schema;
        }
        /// <summary>
        /// Returns the table name used by the data provider.
        /// </summary>
        /// <returns>Returns the table name used by the data provider.</returns>
        public string GetTableName() {
            SQLBuilder sb = new SQLBuilder();
            return sb.BuildFullTableName(Database, Schema, Dataset);
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
