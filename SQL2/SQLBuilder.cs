/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System.Collections.Generic;
using System.Text;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.SQL2 {

    public class SQLBuilder {

        StringBuilder _sb;
        public SQLBuilder() {
            _sb = new StringBuilder();
        }
        public void Add(string s) { _sb.Append(s); }
        public void AddTable(string database, string dbo, string tableName) { _sb.Append(BuildFullTableName(database, dbo, tableName)); }
        public static string GetTable(string database, string dbo, string tableName) { return BuildFullTableName(database, dbo, tableName); }

        public override string ToString() { return _sb.ToString(); }

        public void RemoveLastCharacter() {
            if (_sb.Length > 0)
                _sb.Remove(_sb.Length - 1, 1);// remove last character
        }

        public void AddOrderBy(Dictionary<string, string> visibleColumns, List<DataProviderSortInfo> sorts, int Offset = 0, int Next = 0) {
            Add("ORDER BY ");
            bool first = true;
            foreach (DataProviderSortInfo sortInfo in sorts) {
                if (!first) Add(", ");
                Add(BuildFullColumnName(sortInfo.Field, visibleColumns) + " " + (sortInfo.Order == DataProviderSortInfo.SortDirection.Ascending ? "ASC" : "DESC"));
                first = false;
            }
            if (Offset > 0 || Next > 0)
                Add($" OFFSET {Offset} ROWS FETCH NEXT {Next} ROWS ONLY");
        }

        /// <summary>
        /// Properly escapes a string to be included in a LIKE statement
        /// </summary>
        /// <param name="value">Value to search for</param>
        /// <param name="escapeApostrophe">Whether to escape the apostrophe. Prevents double escaping of apostrophes</param>
        /// <returns>The translated value ready to be used in a LIKE statement</returns>
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
        /// Escapes the apostrophe on strings
        /// </summary>
        /// <param name="sql">SQL statement fragment</param>
        /// <returns>The clean SQL fragment</returns>
        public static string EscapeApostrophe(string sql) {
            sql = sql.Replace("'", "''");
            return sql;
        }

        // TABLE, COLUMN FORMATTING
        // TABLE, COLUMN FORMATTING
        // TABLE, COLUMN FORMATTING

        public static string BuildFullTableName(string tableName) {
            return WrapBrackets(tableName);
        }
        public static string BuildFullColumnName(string tableName, string column) {
            return BuildFullTableName(tableName) + "." + WrapBrackets(column);
        }
        public static string BuildFullTableName(string database, string dbo, string tableName) {
            return $"{WrapBrackets(database)}.{WrapBrackets(dbo)}.{WrapBrackets(tableName)}";
        }
        public static string BuildFullColumnName(string database, string dbOwner, string tableName, string siteColumn) {
            return WrapBrackets(database) + "." + WrapBrackets(dbOwner) + "." + BuildFullColumnName(tableName, siteColumn);
        }
        public static string BuildFullColumnName(string column, Dictionary<string, string> visibleColumns) {
            if (visibleColumns != null) {
                string longColumn;
                if (!visibleColumns.TryGetValue(column, out longColumn))
                    throw new InternalError("Column {0} not found in list of visible columns", column);
                return longColumn;
            } else
                return WrapBrackets(column.Replace('.', '_'));
        }
        public static string WrapBrackets(string token) {
            if (token.StartsWith("["))
                return token;
            else
                return $"[{token}]";
        }
    }
}
