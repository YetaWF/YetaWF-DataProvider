/* Copyright © 2021 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Language;
using YetaWF.Core.Models;
using YetaWF.Core.Support;
using YetaWF.Core.Support.Serializers;
using Microsoft.Data.SqlClient;

namespace YetaWF.DataProvider.SQL {

    internal class SQLHelper {

        public SqlConnection SqlConnection;
        public SqlTransaction? SqlTransaction;
        private List<LanguageData> Languages { get; set; }
        private List<SqlParameter> Params = new List<SqlParameter>();

        public SQLHelper(SqlConnection conn, SqlTransaction? trans, List<LanguageData> languages) {
            SqlConnection = conn;
            SqlTransaction = trans;
            Languages = languages;
        }

        public string DebugInfo {
            get {
#if DEBUG
                SQLBuilder sb = new SQLBuilder();
                sb.Add("-- Debug"); sb.Add(Environment.NewLine);
                foreach (SqlParameter p in Params) {
                    string? val = p.Value?.ToString();
                    if (val != null) val = val.Replace('\r', ' ').Replace('\n', ' ');
                    sb.Add($"-- {p.ParameterName} - {val}"); sb.Add(Environment.NewLine);
                }
                sb.Add("--"); sb.Add(Environment.NewLine);
                return sb.ToString();
#else
                return null;
#endif
            }
        }

        // Create,Fill
        // Create,Fill
        // Create,Fill

        public Type GetDerivedType(string dataType, string assemblyName) {
            try {
                Assembly asm = Assemblies.Load(assemblyName)!;
                return asm.GetType(dataType, true)!;
            } catch (Exception exc) {
                throw new InternalError($"Invalid Type {dataType}/{assemblyName} requested - {ErrorHandling.FormatExceptionMessage(exc)}");
            }
        }
        public T CreateObject<T>(SqlDataReader dr) {
            return (T) CreateObject(dr, typeof(T));
        }
        public T CreateObject<T>(SqlDataReader dr, string dataType, string assemblyName) {
            Type t = GetDerivedType(dataType, assemblyName);
            return (T)CreateObject(dr, t);
        }
        public object CreateObject(SqlDataReader dr, Type tp) {
            object obj = Activator.CreateInstance(tp) !;
            FillObject(dr, obj);
            return obj;
        }
        public void FillObject(SqlDataReader dr, object obj) {
            List<string> columns = new List<string>();
            for (int ci = 0; ci < dr.FieldCount; ci++)
                columns.Add(dr.GetName(ci));
            FillObject(dr, obj, columns);
        }
        private void FillObject(SqlDataReader dr, object container, List<string> columns, string prefix = "") {
            Type tpContainer = container.GetType();
            List<PropertyData> propData = ObjectSupport.GetPropertyData(tpContainer);
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave")) {
                    string colName = prefix + prop.ColumnName;
                    if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        if (columns.Contains(colName)) {
                            object val = dr[colName];
                            if (!(val is System.DBNull)) {
                                byte[] btes = (byte[])val;
                                if (pi.PropertyType == typeof(byte[])) { // truly binary
                                    if (btes.Length > 0)
                                        pi.SetValue(container, btes, null);
                                } else {
                                    object? data = new GeneralFormatter().Deserialize<object>(btes);
                                    pi.SetValue(container, data, null);
                                }
                            }
                        }
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        MultiString ms = prop.GetPropertyValue<MultiString>(container);
                        foreach (LanguageData lang in Languages) {
                            string key = colName + "_" + lang.Id.Replace("-", "_");
                            if (columns.Contains(key)) {
                                object value = dr[key];
                                if (!(value is System.DBNull)) {
                                    string s = (string)value;
                                    if (!string.IsNullOrWhiteSpace(s))
                                        ms[lang.Id] = s;
                                }
                            }
                        }
                    } else if (pi.PropertyType == typeof(System.Drawing.Image)) {
                        throw new InternalError("Image and Bitmap types no longer supported/needed");
                    } else if (columns.Contains(colName)) {
                        object value = dr[colName];
                        pi.SetValue(container, GetValue(pi.PropertyType, value), BindingFlags.Default, null, null, null);
                    } else if (pi.PropertyType.IsClass && ComplexTypeInColumns(columns, colName + "_")) {// This is SLOW so it should be last
                        object? propVal = pi.GetValue(container);
                        if (propVal != null)
                            FillObject(dr, propVal, columns, colName + "_");
                    }
                }
            }
        }

        private bool ComplexTypeInColumns(List<string> columns, string prefix) {
            foreach (string column in columns) {
                if (column.StartsWith(prefix))
                    return true;
            }
            return false;
        }
        public object? GetValue(Type fieldType, object value) {

            object? newValue = null;
            Type? baseType = fieldType.BaseType;
            Type? underlyingType = Nullable.GetUnderlyingType(fieldType);

            // Check if an empty value or an empty string
            if (value == null || value.ToString() == String.Empty)
                return newValue;

            if (fieldType.Equals(value.GetType())) {
                newValue = value;
            } else if (fieldType == typeof(bool)) {
                newValue = (value.ToString() == "1" ||
                            value.ToString()!.ToLower() == "on" ||
                            value.ToString()!.ToLower() == "true" ||
                            value.ToString()!.ToLower() == "yes") ? true : false;
            } else if (underlyingType != null) {// Nullable types
                if (underlyingType == typeof(DateTime))
                    newValue = Convert.ToDateTime(value);
                else if (underlyingType == typeof(TimeSpan))
                    newValue = new TimeSpan(Convert.ToInt64(value));
                else if (underlyingType == typeof(bool))
                    newValue = Convert.ToBoolean(value);
                else if (underlyingType == typeof(short))
                    newValue = Convert.ToInt16(value);
                else if (underlyingType == typeof(int))
                    newValue = Convert.ToInt32(value);
                else if (underlyingType == typeof(long))
                    newValue = Convert.ToInt64(value);
                else if (underlyingType == typeof(decimal))
                    newValue = Convert.ToDecimal(value);
                else if (underlyingType == typeof(double))
                    newValue = Convert.ToDouble(value);
                else if (underlyingType == typeof(float))
                    newValue = Convert.ToSingle(value);
                else if (underlyingType == typeof(ushort))
                    newValue = Convert.ToUInt16(value);
                else if (underlyingType == typeof(uint))
                    newValue = Convert.ToUInt32(value);
                else if (underlyingType == typeof(ulong))
                    newValue = Convert.ToUInt64(value);
                else if (underlyingType == typeof(sbyte))
                    newValue = Convert.ToSByte(value);
                else if (underlyingType == typeof(Guid))
                    newValue = new Guid(Convert.ToString(value)!);
                else
                    throw new InternalError($"Unsupported type {fieldType.FullName}");
            } else if (fieldType == typeof(Guid)) {
                newValue = new Guid(value.ToString()!);
            } else if (fieldType  == typeof(TimeSpan)) {
                newValue = new TimeSpan(Convert.ToInt64(value));
            } else if (baseType != null && fieldType.BaseType == typeof(Enum)) {
                int intEnum;
                if (int.TryParse(value.ToString(), out intEnum))
                    newValue = intEnum;
                else {
                    try {
                        newValue = Enum.Parse(fieldType, value.ToString()!);
                    } catch (Exception) {
                        newValue = Enum.ToObject(fieldType, value);
                    }
                }
            } else {
                try {
                    newValue = Convert.ChangeType(value, fieldType);
                } catch (Exception) { }
            }
            return newValue;
        }

        private bool HasParams {
            get { return Params.Count > 0; }
        }

        // Execute...

        public Task<SqlDataReader> ExecuteReaderAsync(string text) {
            return ExecuteReaderAsync(SqlConnection, SqlTransaction, CommandType.Text, text, Params);
        }
        public Task<object?> ExecuteScalarAsync(string text) {
            return ExecuteScalarAsync(SqlConnection, SqlTransaction, CommandType.Text, text, Params);
        }
        public Task<int> ExecuteNonQueryAsync(string text) {
            return ExecuteNonQueryAsync(SqlConnection, SqlTransaction, CommandType.Text, text, Params);
        }
        public Task<SqlDataReader> ExecuteReaderStoredProcAsync(string sproc) {
            return ExecuteReaderAsync(SqlConnection, SqlTransaction, CommandType.StoredProcedure, sproc, Params);
        }

        private static async Task<SqlDataReader> ExecuteReaderAsync(SqlConnection connection, SqlTransaction? transaction, CommandType commandType, string commandText, List<SqlParameter>? sqlParms) {
            using (SqlCommand cmd = new SqlCommand()) {
                YetaWF.Core.Log.Logging.AddTraceLog(commandText);
                PrepareCommand(cmd, connection, transaction, commandType, commandText, sqlParms);
                if (YetaWFManager.IsSync())
                    return cmd.ExecuteReader();
                else
                    return await cmd.ExecuteReaderAsync();
            }
        }
        private static async Task<object?> ExecuteScalarAsync(SqlConnection connection, SqlTransaction? transaction, CommandType commandType, string commandText, List<SqlParameter>? sqlParms) {
            using (SqlCommand cmd = new SqlCommand()) {
                YetaWF.Core.Log.Logging.AddTraceLog(commandText);
                PrepareCommand(cmd, connection, transaction, commandType, commandText, sqlParms);
                if (YetaWFManager.IsSync())
                    return cmd.ExecuteScalar();
                else
                    return await cmd.ExecuteScalarAsync();
            }
        }
        private static async Task<int> ExecuteNonQueryAsync(SqlConnection connection, SqlTransaction? transaction, CommandType commandType, string commandText, List<SqlParameter>? sqlParms) {
            using (SqlCommand cmd = new SqlCommand()) {
                YetaWF.Core.Log.Logging.AddTraceLog(commandText);
                PrepareCommand(cmd, connection, transaction, commandType, commandText, sqlParms);
                if (YetaWFManager.IsSync())
                    return cmd.ExecuteNonQuery();
                else
                    return await cmd.ExecuteNonQueryAsync();
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        private static void PrepareCommand(SqlCommand command, SqlConnection connection, SqlTransaction? transaction, CommandType commandType, string commandText, List<SqlParameter>? sqlParms) {

            command.Connection = connection;
            command.CommandText = commandText;
            command.CommandTimeout = 300;
            command.CommandType = commandType;

            if (transaction != null) {
                if (transaction.Connection == null) throw new InternalError("The transaction was rolled back or committed");
                command.Transaction = transaction;
            }

            if (sqlParms != null)
                AttachParameters(command, sqlParms);
        }
        private static void AttachParameters(SqlCommand command, List<SqlParameter>? sqlParms) {
            if (sqlParms != null) {
                foreach (SqlParameter p in sqlParms) {
                    if (p != null) {
                        if ((p.Direction == ParameterDirection.InputOutput || p.Direction == ParameterDirection.Input) && p.Value == null)
                            p.Value = DBNull.Value;
                        command.Parameters.Add(p);
                    }
                }
            }
        }

        // EXPR
        // EXPR
        // EXPR

        internal void AddWhereExpr(SQLBuilder sb, string tableName, List<DataProviderFilterInfo> filters, Dictionary<string, string>? visibleColumns) {
            List<DataProviderFilterInfo> list = new List<DataProviderFilterInfo>(filters);
            if (list.Count == 1 && list[0].Filters != null && list[0].Logic == "&&") {
                // topmost entry is just one filter, remove it - it's redundant
                list = new List<DataProviderFilterInfo>(list[0].Filters!);
            }
            AddFiltersExpr(sb, tableName, list, "and", visibleColumns);
        }
        private void AddFiltersExpr(SQLBuilder sb, string tableName, List<DataProviderFilterInfo> filter, string logic, Dictionary<string, string>? visibleColumns) {
            bool firstDone = false;
            foreach (DataProviderFilterInfo f in filter) {
                if (firstDone) {
                    if (logic == "and" || logic == "&&") sb.Add(" AND ");
                    else if (logic == "or" || logic == "||") sb.Add(" OR ");
                    else throw new InternalError("Invalid logic operator {0}", logic);
                }
                bool? isNull = null;
                if (f.Filters != null) {
                    sb.Add("(");
                    AddFiltersExpr(sb, tableName, new List<DataProviderFilterInfo>(f.Filters), f.Logic!, visibleColumns);
                    sb.Add(")");
                } else {
                    string oper = "";
                    object? val = f.Value;
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
                            oper = "LIKE"; val = sb.EscapeForLike((val ?? "").ToString()!, false) + "%"; break;
                        case "notstartswith":
                            isNull = true;
                            oper = "NOT LIKE"; val = sb.EscapeForLike((val ?? "").ToString()!, false) + "%"; break;
                        case "endswith":
                            oper = "LIKE"; val = "%" + sb.EscapeForLike((val ?? "").ToString()!, false); break;
                        case "notendswith":
                            isNull = true;
                            oper = "NOT LIKE"; val = "%" + sb.EscapeForLike((val ?? "").ToString()!, false); break;
                        case "contains":
                            oper = "LIKE"; val = "%" + sb.EscapeForLike((val ?? "").ToString()!, false) + "%"; break;
                        case "notcontains":
                            isNull = true;
                            oper = "NOT LIKE"; val = "%" + sb.EscapeForLike((val ?? "").ToString()!, false) + "%"; break;
                        default:
                            throw new InternalError("Invalid operator {0}", f.Operator);
                    }
                    if (isNull != null) {
                        sb.Add("(");
                        string s = sb.BuildFullColumnName(f.Field!, visibleColumns);
                        AddExpr(sb, s, oper, val);
                        if (isNull == true)
                            sb.Add($" OR {s} IS NULL");
                        else
                            sb.Add($" AND {s} IS NOT NULL");
                        sb.Add(")");
                    } else {
                        AddExpr(sb, sb.BuildFullColumnName(f.Field!, visibleColumns), oper, val);
                    }
                }
                firstDone = true;
            }
        }
        public string Expr(string wherecolumn, string @operator, object? value, bool isSet = false) {
            SQLBuilder sb = new SQLBuilder();
            AddExpr(sb, wherecolumn, @operator, value, isSet);
            return sb.ToString();
        }
        /// <summary>
        /// Adds a parameter to a WHERE statement. Will generate ColumnName {Operator} 'Value' (quotes only added if it is a string)
        /// </summary>
        /// <param name="sb">The SQLBuilder object that holds the current SQL statement.</param>
        /// <param name="wherecolumn">The name of the column</param>
        /// <param name="operator">The operator, e.g. = &lt;= LIKE &lt;&gt; etc.</param>
        /// <param name="value">The value. If it is a string it is properly escaped etc.</param>
        /// <param name="isSet">Identifies this comparison as a set statement. Needed for setting null values.</param>
        internal void AddExpr(SQLBuilder sb, string wherecolumn, string @operator, object? value, bool isSet = false) {
            if (!wherecolumn.Contains(".") && !wherecolumn.StartsWith("["))
                wherecolumn = SQLBuilder.WrapIdentifier(wherecolumn);

            if (value == null) {
                sb.Add(wherecolumn);
                if (@operator == "=") {
                    if (isSet) sb.Add("= NULL"); else sb.Add("IS NULL");
                } else if (@operator == "<>") {
                    if (isSet) sb.Add("<> NULL"); else sb.Add("IS NOT NULL");
                } else
                    throw new InternalError("Invalid operator {0}", @operator);
            } else
                sb.Add($"{wherecolumn} {@operator} {AddTempParam(value)}");
        }
        /// <summary>
        /// Adds a parameter with the provided value and returns the created parameter name
        /// Used when creating dynamic queries and the parameter is not important outside of the immediate query
        /// Used internally to auto created parameters for the WHERE AND OR and other statements
        /// </summary>
        /// <param name="value">The value of the parameter</param>
        /// <returns>The generated name for the parameter</returns>
        public string AddTempParam(object? value) {
            string name = "_tempParam" + Params.Count;
            AddParam(name, value);
            return "@" + name;
        }
        /// <summary>
        /// Adds a named parameter to the query
        /// </summary>
        /// <param name="name">The name of the parameter.</param>
        /// <param name="value">The value of the parameter.</param>
        /// <param name="DbType">The optional type of the parameter value.</param>
        /// <param name="DataTypeName">The optional SQL type of the parameter value. A corresponding User-Defined Type must exist in the database.</param>
        /// <param name="direction">The direction of the parameter (input or output).</param>
        public void AddParam(string name, object? value, ParameterDirection direction = ParameterDirection.Input, SqlDbType? DbType = null, string? DataTypeName = null)/*<<<*/ {

            if (name.StartsWith("@"))
                name = name.Substring(1);

            SqlParameter parm;

            // special handling
            if (value is System.Drawing.Image) {
                throw new InternalError("Image and Bitmap types no longer supported/needed");
            } else if (value is System.String) {
                string s = (string)value ?? "";
                parm = new SqlParameter(name, SqlDbType.NVarChar, s.Length);
                parm.Value = s;
            } else if (value is DateTime) {
                parm = new SqlParameter(name, SqlDbType.DateTime2);
                parm.Value = value;
            } else if (value is TimeSpan) {
                parm = new SqlParameter(name, SqlDbType.BigInt);
                parm.Value = ((TimeSpan)value).Ticks;
            } else {
                if (DataTypeName != null) {
                    parm = new SqlParameter {
                        ParameterName = name,
                        TypeName = DataTypeName,
                        SqlDbType = SqlDbType.Structured,
                        SqlValue = value,
                    };
                } else {
                    if (DbType == null) {
                        parm = new SqlParameter(name, value);
                    } else {
                        if (value == null) {
                            parm = new SqlParameter(name, (SqlDbType)DbType);
                        } else {
                            parm = new SqlParameter {
                                Value = value,
                                ParameterName = name,
                                SqlDbType = (SqlDbType)DbType,
                            };
                        }
                    }
                }
            }
            parm.Direction = direction;//<<<
            Params.Add(parm);
        }
    }
}
