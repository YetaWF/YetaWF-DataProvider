/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System.Collections.Generic;
using System.Text;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.SQL {

    /// <summary>
    /// Helper class used to create dynamic SQL strings.
    /// Similar to the System.Text.StringBuilder class but specialized for SQL statements.
    /// </summary>
    public class SQLBuilder {

        readonly StringBuilder _sb;

        /// <summary>
        /// Constructor.
        /// </summary>
        public SQLBuilder() {
            _sb = new StringBuilder();
        }

        /// <summary>
        /// Appends a string.
        /// </summary>
        /// <param name="s">The string to append.</param>
        public void Add(string s) { _sb.Append(s); }

        /// <summary>
        /// Appends a table name. Renders a fully qualified table name, including database and DB owner, with brackets.
        /// </summary>
        /// <param name="database">The database name.</param>
        /// <param name="dbo">The database owner.</param>
        /// <param name="tableName">The table name.</param>
        public void AddTable(string database, string dbo, string tableName) { _sb.Append(BuildFullTableName(database, dbo, tableName)); }

        /// <summary>
        /// Returns a fully qualified table name, including database and DB owner, with brackets.
        /// </summary>
        /// <param name="database">The database name.</param>
        /// <param name="dbo">The database owner.</param>
        /// <param name="tableName">The table name.</param>
        /// <returns>Returns a fully qualified table name, including database and DB owner, with brackets.</returns>
        public static string GetTable(string database, string dbo, string tableName) { return BuildFullTableName(database, dbo, tableName); }

        /// <summary>
        /// Returns the complete SQL string built using this instance.
        /// </summary>
        /// <returns>Returns the complete SQL string.</returns>
        public override string ToString() { return _sb.ToString(); }

        /// <summary>
        /// Removes the last appended character from the string.
        /// </summary>
        /// <remarks>This can be used when generating lists to remove the last trailing comma, for example.</remarks>
        public void RemoveLastCharacter() {
            if (_sb.Length > 0)
                _sb.Remove(_sb.Length - 1, 1);// remove last character
        }

        /// <summary>
        /// Appends a fully formatted ORDER BY clause based on the provided sort criteria and paging info.
        /// </summary>
        /// <param name="visibleColumns">The collection of columns visible in the table. May be null.</param>
        /// <param name="sorts">A collection describing the sort order.</param>
        /// <param name="Offset">The number of records to skip.</param>
        /// <param name="Next">The number of records to retrieve.</param>
        /// <remarks>
        /// If <paramref name="Offset"/> and <paramref name="Next"/> are specified (not 0),
        /// OFFSET <paramref name="Offset"/> ROWS FETCH NEXT <paramref name="Next"/> ROWS ONLY is appended to the generated ORDER BY clause.
        /// </remarks>
        public void AddOrderBy(Dictionary<string, string> visibleColumns, List<DataProviderSortInfo> sorts, int Offset = 0, int Next = 0) {
            Add(GetOrderBy(visibleColumns, sorts, Offset:Offset, Next: Next));
        }
        /// <summary>
        /// Returns a fully formatted ORDER BY clause based on the provided sort criteria and paging info.
        /// </summary>
        /// <param name="visibleColumns">The collection of columns visible in the table. May be null.</param>
        /// <param name="sorts">A collection describing the sort order.</param>
        /// <param name="Offset">The number of records to skip.</param>
        /// <param name="Next">The number of records to retrieve.</param>
        /// <remarks>
        /// If <paramref name="Offset"/> and <paramref name="Next"/> are specified (not 0),
        /// OFFSET <paramref name="Offset"/> ROWS FETCH NEXT <paramref name="Next"/> ROWS ONLY is appended to the generated ORDER BY clause.
        /// </remarks>
        public string GetOrderBy(Dictionary<string, string> visibleColumns, List<DataProviderSortInfo> sorts, int Offset = 0, int Next = 0) {
            StringBuilder sb = new StringBuilder();
            sb.Append("ORDER BY ");
            bool first = true;
            foreach (DataProviderSortInfo sortInfo in sorts) {
                if (!first) sb.Append(", ");
                sb.Append(BuildFullColumnName(sortInfo.Field, visibleColumns) + " " + (sortInfo.Order == DataProviderSortInfo.SortDirection.Ascending ? "ASC" : "DESC"));
                first = false;
            }
            if (Offset > 0 || Next > 0)
                sb.Append($" OFFSET {Offset} ROWS FETCH NEXT {Next} ROWS ONLY");
            return sb.ToString();
        }

        /// <summary>
        /// Returns a properly escaped string to be included in a LIKE statement.
        /// </summary>
        /// <param name="value">The value to search for.</param>
        /// <param name="escapeApostrophe">Defines whether to escape an apostrophe. Can be used to prevent double escaping of apostrophes.</param>
        /// <returns>Returns the translated value ready to be used in a LIKE statement.</returns>
        public static string EscapeForLike(string value, bool escapeApostrophe = true) {
            string[] specialChars = { "%", "_", "-", "^" };
            string newChars;

            // Escape the [ bracket
            newChars = value.Replace("[", "[[]");

            // Replace the special chars
            foreach (string t in specialChars) {
                newChars = newChars.Replace(t, "[" + t + "]");
            }

            // Escape the apostrophe if requested
            if (escapeApostrophe)
                newChars = EscapeApostrophe(newChars);

            return newChars;
        }
        /// <summary>
        /// Escapes apostrophes in strings.
        /// </summary>
        /// <param name="sql">A SQL statement fragment.</param>
        /// <returns>Returns the clean SQL fragment, with escaped apostrophes.</returns>
        public static string EscapeApostrophe(string sql) {
            sql = sql.Replace("'", "''");
            return sql;
        }

        // TABLE, COLUMN FORMATTING
        // TABLE, COLUMN FORMATTING
        // TABLE, COLUMN FORMATTING

        /// <summary>
        /// Returns a formatted table name or a formatted database name, database owner and table name, with brackets.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        /// <returns>Returns a formatted table or a formatted name database name, database owner and table name, with brackets.</returns>
        /// <remarks>The result is bracketed. This method considers whether any of the parameters is already bracketed in which case no further brackets are added.</remarks>
        public static string BuildFullTableName(string tableName) {
            return WrapBrackets(tableName);
        }
        /// <summary>
        /// Returns a formatted table name and column name, with brackets.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        /// <param name="column">The column name.</param>
        /// <returns>Returns a formatted table name and column name, with brackets.</returns>
        /// <remarks>The result is bracketed. This method considers whether either parameter is already bracketed in which case no further brackets are added.</remarks>
        public static string BuildFullColumnName(string tableName, string column) {
            return BuildFullTableName(tableName) + "." + WrapBrackets(column);
        }
        /// <summary>
        /// Returns a formatted table name or a formatted database name, database owner and table name, with brackets.
        /// </summary>
        /// <param name="database">The database name.</param>
        /// <param name="dbo">The database owner.</param>
        /// <param name="tableName">The table name.</param>
        /// <returns>Returns a formatted table or a formatted name database name, database owner and table name, with brackets.</returns>
        /// <remarks>The result is bracketed. This method considers whether any of the parameters is already bracketed in which case no further brackets are added.</remarks>
        public static string BuildFullTableName(string database, string dbo, string tableName) {
            return $"{WrapBrackets(database)}.{WrapBrackets(dbo)}.{WrapBrackets(tableName)}";
        }
        /// <summary>
        /// Returns a formatted column name or a formatted database name, database owner, table name and column name, with brackets.
        /// </summary>
        /// <param name="database">The database name.</param>
        /// <param name="dbOwner">The database owner.</param>
        /// <param name="tableName">The table name.</param>
        /// <param name="column">The column name.</param>
        /// <returns>Returns a formatted column name or a formatted database name, database owner, table name and column name, with brackets.</returns>
        /// <remarks>The result is bracketed. This method considers whether any of the parameters is already bracketed in which case no further brackets are added.</remarks>
        public static string BuildFullColumnName(string database, string dbOwner, string tableName, string column) {
            return WrapBrackets(database) + "." + WrapBrackets(dbOwner) + "." + BuildFullColumnName(tableName, column);
        }
        /// <summary>
        /// Returns a formatted column name or a formatted database name, database owner, table name and column name, with brackets.
        /// </summary>
        /// <param name="column">The column name.</param>
        /// <param name="visibleColumns">The collection of columns visible in the table.</param>
        /// <returns>Returns a formatted column name or a formatted database name, database owner, table name and column name, with brackets.</returns>
        /// <remarks>The result is bracketed. This method considers whether any of the parameters is already bracketed in which case no further brackets are added.</remarks>
        public static string BuildFullColumnName(string column, Dictionary<string, string> visibleColumns) {
            if (visibleColumns != null) {
                string longColumn;
                if (!visibleColumns.TryGetValue(column, out longColumn))
                    throw new InternalError($"Column {column} not found in list of visible columns");
                return longColumn;
            } else {
                if (!column.StartsWith("["))
                    column = column.Replace('.', '_');
                return WrapBrackets(column);
            }
        }
        /// <summary>
        /// Returns a properly bracketed string.
        /// </summary>
        /// <param name="token">The text to bracket.</param>
        /// <returns>Returns a properly bracketed string.</returns>
        /// <remarks>The result is bracketed. This method considers whether the parameter is already bracketed in which case no further brackets are added.</remarks>
        public static string WrapBrackets(string token) {
            if (token.StartsWith("["))
                return token;
            else
                return $"[{token}]";
        }
    }
}
