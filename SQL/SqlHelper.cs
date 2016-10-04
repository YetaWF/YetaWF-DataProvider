/* Copyright © 2016 Softel vdm, Inc. - http://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Language;
using YetaWF.Core.Support;
using YetaWF.DataProvider;

namespace BigfootSQL {
    /// <summary>
    /// This is a simple SQL syntax builder helper class. It aids in the creation of a SQL statement
    /// by auto creating parameters, as well as preventing against injection attacks etc. It also uses
    /// the DAAB class and ObjectHelper classes to execute the query and hydrate the Model objects.
    ///
    /// It was designed with simplicity and speed in mind. It is a great replacement to writing
    /// directly against the ADO.NET providers. It is not meant to be a full ORM but rather a rapid query
    /// execution and object hydration helper.
    ///
    /// It uses a fluid interface for simplicity of code. You must know how to write SQL as it is a light
    /// SQL code builder while automating the rest. It does not generate SQL for you, rather it makes the
    /// writing, executing, and results mapping simple.
    ///
    /// Examples (Assumes there is a DB variable of type SqlHelper):
    ///     Select a list of orders:
    ///         List<OrderListItem> obj;
    ///         obj = DB.SELECT("OrderID, OrderDate, ShipToCity").FROM("Orders").WHERE("ShipToState","FL").ExecuteCollection<OrderListItem>();
    ///
    ///     Select a single value typed to the correct type:
    ///         DateTime d;
    ///         d = DB.SELECT("OrderDate").FROM("Orders").WHERE("OrderID",OrderID).ExecuteScalar<datetime>()
    ///
    /// It has several other Execute methods to retrieve DataReaders, DataSets, and many others. Also has ExecuteNonQuery for executing
    /// void queries.
    /// </summary>
    public class SqlHelper {

        StringBuilder _sql = new StringBuilder();
        List<SqlParameter> _params = new List<SqlParameter>();

        public SqlConnection SqlConnection;
        public SqlTransaction SqlTransaction;
        public List<LanguageData> Languages;

        public SqlHelper(SqlConnection sqlConnection, SqlTransaction sqlTransaction, List<LanguageData> languages) {
            SqlConnection = sqlConnection;
            SqlTransaction = sqlTransaction;
            Languages = languages;
        }
        public SqlHelper(SqlConnection sqlConnection, List<LanguageData> languages) {
            SqlConnection = sqlConnection;
            SqlTransaction = null;
            Languages = languages;
        }

        public StringBuilder RawBuilder {
            get { return _sql; }
        }

        /// <summary>
        /// Add literal SQL statement to the query
        /// </summary>
        /// <param name="sql">SQL Fragment to add</param>
        public SqlHelper Add(string sql) {
            return Append(sql);
        }

        /// <summary>
        /// Add a parameter to a WHERE statement. Will generate ColumnName = 'Value' (quotes only added if it is a string)
        /// </summary>
        /// <param name="wherecolumn">The name of the column to search</param>
        /// <param name="value">The value to search for</param>
        public SqlHelper Add(string wherecolumn, object value) {
            return Add(wherecolumn, "=", value, false);
        }

        /// <summary>
        /// Add a parameter to a WHERE statement. Will generate ColumnName {Operator} 'Value' (quotes only added if it is a string)
        /// </summary>
        /// <param name="wherecolumn">The of the column to search</param>
        /// <param name="value">The value to search for. If it is a string it is properly escaped etc.</param>
        /// /// <param name="isSet">Identifies this comparison as a set statement. Needed for setting null values</param>
        public SqlHelper Add(string wherecolumn, object value, bool isSet) {
            return Add(wherecolumn, "=", value, isSet);
        }

        /// <summary>
        /// Add a parameter to a WHERE statement. Will generate ColumnName {Operator} 'Value' (quotes only added if it is a string)
        /// </summary>
        /// <param name="wherecolumn">The of the column to search</param>
        /// <param name="operator">The operator for the search. e.g. = <= LIKE <> etc.</param>
        /// <param name="value">The value to search for. If it is a string it is properly escaped etc.</param>
        public SqlHelper Add(string wherecolumn, string @operator, object value) {
            return Add(wherecolumn, @operator, value, false);
        }

        /// <summary>
        /// Add a parameter to a WHERE statement. Will generate ColumnName {Operator} 'Value' (quotes only added if it is a string)
        /// </summary>
        /// <param name="wherecolumn">The # of the column to search</param>
        /// <param name="operator">The operator for the search. e.g. = <= LIKE <> etc.</param>
        /// <param name="value">The value to search for. If it is a string it is properly escaped etc.</param>
        /// <param name="isSet">Identifies this comparison as a set statement. Needed for setting null values</param>
        public SqlHelper Add(string wherecolumn, string @operator, object value, bool isSet) {
            if (!wherecolumn.Contains(".") && !wherecolumn.StartsWith("["))
                wherecolumn = SQLDataProviderImpl.WrapBrackets(wherecolumn);

            if (value == null) {
                Add(wherecolumn);
                if (@operator == "=")
                    return isSet ? Add("= NULL") :
                               Add("IS NULL");
                else if (@operator == "<>")
                    return isSet ? Add("<> NULL") :
                               Add("IS NOT NULL");
                else
                    throw new InternalError("Invalid operator {0}", @operator);
            } else
                return Add(wherecolumn).Add(@operator).Add(AddTempParam(value));
        }

        public SqlHelper SELECT(string sql) {
            return Add("SELECT " + sql);
        }

        public SqlHelper SELECT_ALL_FROM(string tablename) {
            return Add("SELECT * FROM").Add(tablename);
        }

        public SqlHelper SELECT(params string[] columns) {
            var s = "SELECT ";
            var firstcolumn = true;
            foreach (string c in columns) {
                if (firstcolumn) {
                    s += c;//<< was columns
                    firstcolumn = false;
                } else {
                    s += ", " + c;
                }
            }
            return Add(s);
        }

        public SqlHelper SELECT_IDENTITY() {
            return Add("SELECT @@IDENTITY");
        }

        public SqlHelper INNERJOIN(string sql) {
            return Add("INNER JOIN " + sql);
        }

        public SqlHelper LEFTJOIN(string sql) {
            return Add("LEFT JOIN " + sql);
        }

        public SqlHelper ON() {
            return Add("ON ");
        }

        public SqlHelper ON(string leftcolumn, string rightcolumn) {
            return Add("ON " + leftcolumn + " = " + rightcolumn);
        }
        public SqlHelper ANDON(string leftcolumn, string rightcolumn) {
            return Add("AND " + leftcolumn + " = " + rightcolumn);
        }

        public SqlHelper FROM(string tableName) {
            return Add("FROM " + SQLDataProviderImpl.WrapBrackets(tableName));
        }
        public SqlHelper FROM(string databaseName, string dbOwner, string tableName) {
            return Add("FROM " + MakeFullTableName(databaseName, dbOwner, tableName));
        }

        public SqlHelper WHERE() {
            return Add("WHERE");
        }

        public SqlHelper WHERE(string columnname, object value) {
            return Add("WHERE").Add(columnname, value);
        }
        public void WHERE_EXPR(string tableName, List<DataProviderFilterInfo> filter, Dictionary<string, string> visibleColumns) {
            if (filter == null) return;
            List<DataProviderFilterInfo> list = new List<DataProviderFilterInfo>(filter);
            if (list.Count == 1 && list[0].Filters != null && list[0].Logic == "&&") {
                // topmost entry is just one filter, remove it - it's redundant
                list = new List<DataProviderFilterInfo>(list[0].Filters);
            }
            AddFiltersExpr(tableName, list, "and", visibleColumns);
        }
        private void AddFiltersExpr(string tableName, List<DataProviderFilterInfo> filter, string logic, Dictionary<string, string> visibleColumns) {
            bool firstDone = false;
            foreach (var f in filter) {
                if (firstDone) {
                    if (logic == "and" || logic == "&&") AND();
                    else if (logic == "or" || logic == "||") OR();
                    else throw new InternalError("Invalid logic operator {0}", logic);
                }
                bool? isNull = null;
                if (f.Filters != null) {
                    OP();
                    AddFiltersExpr(tableName, new List<DataProviderFilterInfo>(f.Filters), f.Logic, visibleColumns);
                    CP();
                } else {
                    string oper = "";
                    object val = f.Value;
                    if (val != null && val.GetType() == typeof(DateTime)) {
                        val = ((DateTime)val).ToString("yyyy-MM-dd HH:mm:ss");
                    }
                    switch (f.Operator.ToLower()) {
                        case "eq":
                        case "==":
                        case "=":
                            oper = "="; break;
                        case "neq":
                        case "<>":
                        case "!=":
                            if (val != null)
                                isNull = true;
                            oper = "<>"; break;
                        case "lt":
                        case "<":
                            isNull = true;
                            oper = "<"; break;
                        case "lte":
                        case "le":
                        case "<=":
                            isNull = true;
                            oper = "<="; break;
                        case "gt":
                        case ">":
                            isNull = false;
                            oper = ">"; break;
                        case "gte":
                        case "ge":
                        case ">=":
                            isNull = false;
                            oper = ">="; break;
                        case "startswith":
                            oper = "LIKE"; val = EscapeForLike((val ?? "").ToString(), false) + "%"; break;
                        case "notstartswith":
                            isNull = true;
                            oper = "NOT LIKE"; val = EscapeForLike((val ?? "").ToString(), false) + "%"; break;
                        case "endswith":
                            oper = "LIKE"; val = "%" + EscapeForLike((val ?? "").ToString(), false); break;
                        case "notendswith":
                            isNull = true;
                            oper = "NOT LIKE"; val = "%" + EscapeForLike((val ?? "").ToString(), false); break;
                        case "contains":
                            oper = "LIKE"; val = "%" + EscapeForLike((val ?? "").ToString(), false) + "%"; break;
                        case "notcontains":
                            isNull = true;
                            oper = "NOT LIKE"; val = "%" + EscapeForLike((val ?? "").ToString(), false) + "%"; break;
                        default:
                            throw new InternalError("Invalid operator {0}", f.Operator);
                    }
                    if (isNull != null) {
                        OP();
                        string s = MakeFullColumnName(f.Field, visibleColumns);
                        Add(s, oper, val);
                        if (isNull == true) {
                            OR();
                            Add(s + " IS NULL");
                        } else {
                            AND();
                            Add(s + " IS NOT NULL");
                        }
                        CP();
                    } else {
                        Add(MakeFullColumnName(f.Field, visibleColumns), oper, val);
                    }
                }
                firstDone = true;
            }
        }
        protected string MakeFullTableName(string databaseName, string dbOwner, string tableName) {
            return SQLDataProviderImpl.WrapBrackets(databaseName) + "." + SQLDataProviderImpl.WrapBrackets(dbOwner) + "." + SQLDataProviderImpl.WrapBrackets(tableName);
        }
        protected string MakeFullColumnName(string column, Dictionary<string, string> visibleColumns) {
            if (visibleColumns != null) {
                string longColumn;
                if (!visibleColumns.TryGetValue(column, out longColumn))
                    throw new InternalError("Column {0} not found in list of visible columns", column);
                return longColumn;
            } else
                return SQLDataProviderImpl.WrapBrackets(column.Replace('.', '_'));
        }

        public SqlHelper ORDERBY(string sql, bool Asc = true, int Offset = 0, int Next = 0) {
            Add("ORDER BY " + sql);
            if (!Asc)
                Add(" DESC");
            if (Offset > 0 || Next > 0)
                Add(string.Format(" OFFSET {0} ROWS FETCH NEXT {1} ROWS ONLY", Offset, Next));
            return this;
        }
        public SqlHelper ORDERBY(Dictionary<string, string> visibleColumns, List<DataProviderSortInfo> sorts, bool Asc = true, int Offset = 0, int Next = 0) {
            if (sorts == null && sorts.Count == 0) throw new InternalError("No sort order given");
            Add("ORDER BY ");
            bool first = true;
            foreach (DataProviderSortInfo sortInfo in sorts) {
                if (!first) Add(",");
                Add(MakeFullColumnName(sortInfo.Field, visibleColumns) + " " + sortInfo.GetOrder());
                first = false;
            }
            if (Offset > 0 || Next > 0)
                Add(string.Format(" OFFSET {0} ROWS FETCH NEXT {1} ROWS ONLY", Offset, Next));
            return this;
        }

        public SqlHelper INSERTINTO(string tablename, string columns) {
            return Add("INSERT INTO " + tablename + "(" + columns + ")");
        }

        public SqlHelper OP() {
            return Add("(");
        }

        public SqlHelper OP(string wherecolumn, object value) {
            return Add("(").Add(wherecolumn, value);
        }

        public SqlHelper CP() {
            return Add(")");
        }

        bool updateStarted = false;

        public SqlHelper UPDATE(string sql) {
            updateStarted = true;
            return Add("UPDATE " + sql);
        }

        public SqlHelper SET(string columnname, object value) {
            if (updateStarted)
                Add("SET");
            updateStarted = false;

            if (!_sql.ToString().TrimEnd().EndsWith(" SET") &&
                !_sql.ToString().TrimEnd().EndsWith(","))
                Add(",");

            Add(columnname, value, true);

            return this;
        }

        public SqlHelper DELETE(string sql) {
            return Add("DELETE " + sql);
        }

        public SqlHelper DELETEFROM(string sql) {
            return Add("DELETE FROM " + sql);
        }

        public SqlHelper AND() {
            return Add("AND");
        }

        public SqlHelper AND(string column, object value) {
            return Add("AND").Add(column, value);
        }

        public SqlHelper OR() {
            return Add("OR");
        }

        public SqlHelper VALUES(string sql) {
            return VALUES_START().Add(sql).VALUES_END();
        }

        public SqlHelper VALUES_START() {
            return Add("VALUES ( ");
        }

        public SqlHelper VALUES_END() {
            return Add(" ) ");
        }

        /// <summary>
        /// Adds a parameter with the provided value and returns the created parameter name
        /// Used when creating dynamic queries and the parameter is not important outside of the immediate query
        /// Used internally to auto created parameters for the WHERE AND OR and other statements
        /// </summary>
        /// <param name="value">The value of the parameter</param>
        /// <returns>The generated name for the parameter</returns>
        public string AddTempParam(object value) {
            var name = "_tempParam" + _params.Count;
            AddParam(name, value);
            return "@" + name;
        }

        /// <summary>
        /// Adds a named parameter to the query
        /// </summary>
        /// <param name="name">The name of the parameter</param>
        /// <param name="value">The value of the parameter</param>
        public SqlHelper AddParam(string name, object value, ParameterDirection direction = ParameterDirection.Input)//<<<
        {
            if (name.StartsWith("@")) name = name.Substring(1);

            SqlParameter parm;

            // special handling
            if (value is System.Drawing.Image) {
                // for image parameters - Note that images will always be saves as jpeg (this could be changed)
                System.Drawing.Image img = (System.Drawing.Image)value;
                System.IO.MemoryStream ms = new System.IO.MemoryStream();
                img.Save(ms, System.Drawing.Imaging.ImageFormat.Jpeg);
                ms.Close();
                value = ms.ToArray();
                parm = new SqlParameter(name, value);
            } else if (value is System.Data.Linq.Binary) {
                // for Linq.Binary parameters
                System.Data.Linq.Binary lb = (System.Data.Linq.Binary)value;
                parm = new SqlParameter(name, lb.ToArray());
            } else if (value is System.String) {
                string s = (string)value ?? "";
                parm = new SqlParameter(name, SqlDbType.NVarChar, s.Length);
                parm.Value = s;
            } else if (value is DateTime) {
                parm = new SqlParameter(name, SqlDbType.DateTime2);
                parm.Value = value;
            } else {
                parm = new SqlParameter(name, value);
            }
            parm.Direction = direction;//<<<
            _params.Add(parm);

            return this;
        }

        /// <summary>
        /// Clear the current query
        /// </summary>
        public void Clear() {
            _sql = new StringBuilder();
            _params = new List<SqlParameter>();
        }

        /// <summary>
        /// Auto writes the finished statement as close as possible.
        /// </summary>
        public override string ToString() {
#if DEBUG
            // uncomment for detailed debugging
            // Debug.WriteLine(DebugSql);
#endif
            return _sql.ToString();
        }

        private SqlHelper Append(string sql) {
            if (_sql.Length > 0 && _sql[_sql.Length - 1] != ' ')
                _sql.Append(" ");
            _sql.Append(sql);
            return this;
        }

        //private void AddIfNotFound(string statement)
        //{
        //    if (_sql.ToString().IndexOf(statement) == -1)
        //    {
        //        Add(statement);
        //    }
        //}

        /// <summary>
        /// Creates an executable SQL statement including declaration of SQL parameters for debugging purposes.
        /// </summary>
        public string DebugSql {
            get {
                var value = "====NEW QUERY====\r\n";
                foreach (SqlParameter param in _params) {
                    var addQuotes = (param.SqlDbType == SqlDbType.NVarChar);
                    value += "DECLARE @" + param.ParameterName + " " + param.SqlDbType;
                    if (param.SqlDbType == SqlDbType.NVarChar || param.SqlDbType == SqlDbType.VarChar)
                        value += "(" + param.Size.ToString() + ")";
                    if (param.Value == null)
                        value += " SET @" + param.ParameterName + " = NULL";
                    else
                        value += " SET @" + param.ParameterName + " = " +
                                    ((addQuotes) ? "'" + EscapeApostrophe(param.Value.ToString()) + "'"
                                                 : EscapeApostrophe(param.Value.ToString()));
                    value += "\r\n";
                }
                value += _sql + "\r\n";

                //Add \r\n before each of these words
                string[] words = { "SELECT", "FROM", "WHERE", "INNER JOIN", "LEFT JOIN", "ORDER BY", "GROUP BY", "DECLARE",
                                     "SET", "VALUES", "INSERT INTO", "DELETE FROM", "UPDATE" };
                foreach (string w in words)
                    value = value.Replace(w, "\r\n" + w);

                // Return the value
                return value;
            }
        }

        /// <summary>
        /// Escapes the apostrophe on strings
        /// </summary>
        /// <param name="sql">SQL statement fragment</param>
        /// <returns>The clean SQL fragment</returns>
        public static string EscapeApostrophe(string sql) {
            sql = sql.Replace("'", "''");
            return sql;
        }

        /// <summary>
        /// Properly escapes a string to be included in a LIKE statement
        /// </summary>
        /// <param name="value">Value to search for</param>
        /// <param name="escapeApostrophe">Whether to escape the apostrophe. Prevents double escaping of apostrophes</param>
        /// <returns>The translated value ready to be used in a LIKE statement</returns>
        public static string EscapeForLike(string value, bool escapeApostrophe = true) {
            string[] specialChars = {"%", "_", "-", "^"};
            string newChars;

            // Escape the [ bracket
            newChars = value.Replace("[", "[[]");

            // Replace the special chars
            foreach (string t in specialChars){
                newChars = newChars.Replace(t, "[" + t + "]");
            }

            // Escape the apostrophe if requested
            if (escapeApostrophe)
                newChars = EscapeApostrophe(newChars);

            return newChars;
        }

        private bool HasParams {
            get { return _params.Count > 0; }
        }

        /// <summary>
        /// Executes the query and returns a Scalar value
        /// </summary>
        /// <returns>Object (null when dbnull value is returned)</returns>
        public object ExecuteScalar() {
            object rvalue;
            if (SqlTransaction != null) {
                rvalue = (HasParams)
                    ? DAAB.ExecuteScalar(SqlTransaction, CommandType.Text, ToString(), _params.ToArray())
                    : DAAB.ExecuteScalar(SqlTransaction, CommandType.Text, ToString());
            } else {
                rvalue = (HasParams)
                    ? DAAB.ExecuteScalar(SqlConnection, CommandType.Text, ToString(), _params.ToArray())
                    : DAAB.ExecuteScalar(SqlConnection, CommandType.Text, ToString());
            }
            if (rvalue == DBNull.Value) rvalue = null;

            return rvalue;
        }

        /// <summary>
        /// Executes the query and returns a Scalar value for the specific generic value
        /// </summary>
        /// <returns>A typed object of T</returns>
        public T ExecuteScalar<T>() {
            object rvalue = ExecuteScalar();
            if (rvalue != null) {
                var tc = TypeDescriptor.GetConverter(typeof(T));
                return (T)tc.ConvertFromInvariantString(rvalue.ToString());
            }

            return default(T);
        }

        /// <summary>
        /// Appends a SELECT @@IDENTITY statement to the query and then executes
        /// </summary>
        /// <returns>The identity of the just inserted record</returns>
        public int ExecuteScalarIdentity() {
            SELECT_IDENTITY();
            return ExecuteScalarInt();
        }

        /// <summary>
        /// Executes the query and returns a scalar value of type int
        /// </summary>
        public int ExecuteScalarInt() {
            return ExecuteScalar<int>();
        }

        /// <summary>
        /// Executes a query that does not return a value
        /// </summary>
        public int ExecuteNonquery() {
            if (SqlTransaction != null) {
                return (HasParams)
                    ? DAAB.ExecuteNonQuery(SqlTransaction, CommandType.Text, ToString(), _params.ToArray())
                    : DAAB.ExecuteNonQuery(SqlTransaction, CommandType.Text, ToString());
            } else {
                return (HasParams)
                    ? DAAB.ExecuteNonQuery(SqlConnection, CommandType.Text, ToString(), _params.ToArray())
                    : DAAB.ExecuteNonQuery(SqlConnection, CommandType.Text, ToString());
            }
        }

        /// <summary>
        /// Executes a query and hydrates an object with the result
        /// </summary>
        /// <typeparam name="T">The type of the object to hydrate and return</typeparam>
        /// <returns>I hydrated object of the type specified</returns>
        public T ExecuteObject<T>() {
            SqlDataReader reader = ExecuteReader();
            ObjectHelper objHelper = new ObjectHelper(Languages);
            T t = objHelper.FillObject<T>(reader);
            reader.Close();
            return t;
        }

        /// <summary>
        /// Executes the query and maps the results to a collection of objects
        /// of the type specified through the generic argument
        /// </summary>
        /// <typeparam name="T">The of object for the collection</typeparam>
        /// <returns>A collection of T</returns>
        public List<T> ExecuteCollection<T>() {
            SqlDataReader reader = ExecuteReader();
            ObjectHelper objHelper = new ObjectHelper(Languages);
            List<T> list = objHelper.FillCollection<T>(reader);
            reader.Close();
            return list;
        }

        /// <summary>
        /// Executes the query and returns a DataReader
        /// </summary>
        public SqlDataReader ExecuteReader() {
            AddParam("SELECT_TOTALRECORDS", 0, ParameterDirection.InputOutput);
            _sql.AppendFormat(" SET @SELECT_TOTALRECORDS = @@ROWCOUNT\n");//<<<<
            if (SqlTransaction != null) {
                return (HasParams)
                       ? DAAB.ExecuteReader(SqlTransaction, CommandType.Text, ToString(), _params.ToArray())
                       : DAAB.ExecuteReader(SqlTransaction, CommandType.Text, ToString());
            } else {
                return (HasParams)
                       ? DAAB.ExecuteReader(SqlConnection, CommandType.Text, ToString(), _params.ToArray())
                       : DAAB.ExecuteReader(SqlConnection, CommandType.Text, ToString());
            }
        }
    }
}
