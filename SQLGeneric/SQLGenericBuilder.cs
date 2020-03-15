/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System.Text;
using YetaWF.Core.Extensions;

namespace YetaWF.DataProvider.SQLGeneric {

    /// <summary>
    /// Helper class used to create dynamic SQL strings.
    /// Similar to the System.Text.StringBuilder class but specialized for SQL statements.
    /// </summary>
    public abstract class SQLGenericBuilder {

        protected readonly StringBuilder _sb;

        /// <summary>
        /// Constructor.
        /// </summary>
        public SQLGenericBuilder() {
            _sb = new StringBuilder();
        }

        /// <summary>
        /// Appends a string.
        /// </summary>
        /// <param name="s">The string to append.</param>
        public void Add(string s) { _sb.Append(s); }

        /// <summary>
        /// Appends a string.
        /// </summary>
        /// <param name="s">The string to append.</param>
        public void Append(string s) { _sb.Append(s); }

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
        public void RemoveLastComma() {
            _sb.RemoveLastComma();
        }

        // TABLE, COLUMN FORMATTING
        // TABLE, COLUMN FORMATTING
        // TABLE, COLUMN FORMATTING

        /// <summary>
        /// Returns a formatted table name or a formatted database name, database schema and table name, with brackets.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        /// <returns>Returns a formatted table or a formatted name database name, database schema and table name, with brackets.</returns>
        /// <remarks>The result is bracketed. This method considers whether any of the parameters is already bracketed in which case no further brackets are added.</remarks>
        public abstract string BuildFullTableName(string tableName);
        /// <summary>
        /// Returns a formatted table name and column name, with brackets.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        /// <param name="column">The column name.</param>
        /// <returns>Returns a formatted table name and column name, with brackets.</returns>
        /// <remarks>The result is bracketed. This method considers whether either parameter is already bracketed in which case no further brackets are added.</remarks>
        public abstract string BuildFullColumnName(string tableName, string column);
        /// <summary>
        /// Returns a formatted table name or a formatted database name, database schema and table name, with brackets.
        /// </summary>
        /// <param name="database">The database name.</param>
        /// <param name="schema">The database schema.</param>
        /// <param name="tableName">The table name.</param>
        /// <returns>Returns a formatted table or a formatted name database name, database schema and table name, with brackets.</returns>
        /// <remarks>The result is bracketed. This method considers whether any of the parameters is already bracketed in which case no further brackets are added.</remarks>
        public abstract string BuildFullTableName(string database, string schema, string tableName);
        /// <summary>
        /// Returns a formatted column name or a formatted database name, database schema, table name and column name, with brackets.
        /// </summary>
        /// <param name="database">The database name.</param>
        /// <param name="schema">The database schema.</param>
        /// <param name="tableName">The table name.</param>
        /// <param name="column">The column name.</param>
        /// <returns>Returns a formatted column name or a formatted database name, database schema, table name and column name, with brackets.</returns>
        /// <remarks>The result is bracketed. This method considers whether any of the parameters is already bracketed in which case no further brackets are added.</remarks>
        public abstract string BuildFullColumnName(string database, string schema, string tableName, string column);
    }
}
