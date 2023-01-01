/* Copyright © 2023 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System.Threading.Tasks;

namespace YetaWF.DataProvider.PostgreSQL {

    /// <summary>
    /// This interface is implemented by the PostgreSQL low-level data provider and can be used
    /// by application data providers to gain access to PostgreSQL specific information.
    /// </summary>
    /// <remarks>
    /// An application data provider using this interface is then of course limited to PostgreSQL.
    ///
    /// This is typically used by application data providers to generate specialized queries that would not be possible with the
    /// built in data provider.
    /// </remarks>
    public interface IPostgreSQLTableInfo {
        /// <summary>
        /// Returns the PostgreSQL connection string used by the data provider.
        /// </summary>
        /// <returns>Returns the PostgreSQL connection string used by the data provider.</returns>
        string GetConnectionString();
        /// <summary>
        /// Returns the database name used by the data provider.
        /// </summary>
        /// <returns>Returns the PostgreSQL database name used by the data provider.</returns>
        string GetDatabaseName();
        /// <summary>
        /// Returns the database owner used by the data provider.
        /// </summary>
        /// <returns>Returns the database owner used by the data provider.</returns>
        string GetSchema();
        /// <summary>
        /// Returns the table name used by the data provider.
        /// </summary>
        /// <returns>Returns the fully qualified table name used by the data provider.</returns>
        string GetTableName();
        /// <summary>
        /// Replaces search text in a PostgreSQL string fragment with the table name used by the data provider.
        /// </summary>
        /// <param name="text">A PostgreSQL fragment where occurrences of <paramref name="searchText"/> are replaced by the table name used by the data provider.</param>
        /// <param name="searchText">The text searched in <paramref name="text"/> that is replaced by the table name used by the data provider.</param>
        /// <returns>Returns the PostgreSQL string fragment with <paramref name="searchText"/> replaced by the table name used by the data provider.</returns>
        string ReplaceWithTableName(string text, string searchText);
        /// <summary>
        /// Replaces search text in a PostgreSQL string fragment with the language used by the data provider.
        /// </summary>
        /// <param name="text">A PostgreSQL fragment where occurrences of <paramref name="searchText"/> are replaced by the language used by the data provider.</param>
        /// <param name="searchText">The text searched in <paramref name="text"/> that is replaced by the language used by the data provider.</param>
        /// <returns>Returns the PostgreSQL string fragment with <paramref name="searchText"/> replaced by the language used by the data provider.</returns>
        string ReplaceWithLanguage(string text, string searchText);
        /// <summary>
        /// Opens the underlying database. This call is required if the data provider is only used to execute SQL statements directly.
        /// </summary>
        Task EnsureOpenAsync();
    }
}
