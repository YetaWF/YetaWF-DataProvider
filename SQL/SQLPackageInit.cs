/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider;
using YetaWF.Core.IO;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;
#if MVC6
using Microsoft.Data.SqlClient;
#else
using System.Data.SqlClient;
#endif

namespace YetaWF.DataProvider.SQL {

    /// <summary>
    /// Base class to implement executing all SQL procedures that are located in a package's Addons/_Main/Sql folder.
    /// </summary>
    /// <remarks>This is used by derived classes to implement executing all SQL procedures that are located in a package's Addons/_Main/Sql folder.
    ///
    /// Can be used to create tables, add stored procedures, etc.
    ///
    /// All files in the package's Addons/_Main/Sql folder with the extension Sql are executed when package models are installed.</remarks>
    public abstract class SQLPackageInit : IDisposable, IInstallableModel {

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose() { }

        /// <summary>
        /// Executes all SQL procedures that are located in the package's Addons/_Main/Sql folder.
        /// </summary>
        /// <param name="package">The package.</param>
        public async Task InitializeAsync(Package package) {

            string connString = WebConfigHelper.GetValue<string>(package.AreaName, SQLBase.SQLConnectString);
            if (string.IsNullOrWhiteSpace(connString))
                throw new InternalError($"No {SQLBase.SQLConnectString} connection string found for package {package.AreaName} - must be explicitly specified");

            string path = Path.Combine(package.AddonsFolder, "_Main", "Sql");
            if (!Directory.Exists(path))
                return;
            string[] files = Directory.GetFiles(path, "*.sql");

            using (SqlConnection conn = new SqlConnection(connString)) {
                conn.Open();
                foreach (string file in files) {
                    string text = await FileSystem.FileSystemProvider.ReadAllTextAsync(file);

                    // use appsettings variables
                    Variables vars = new Variables(null, WebConfigHelper.Variables);
                    text = vars.ReplaceVariables(text);

                    List<string> batches = reGo.Split(text).ToList();

                    using (SqlCommand cmd = new SqlCommand()) {

                        foreach (string batch in batches) {
                            cmd.Connection = conn;
                            cmd.CommandText = batch;
                            cmd.CommandTimeout = 300;
                            cmd.CommandType = System.Data.CommandType.Text;

                            try {
                                if (YetaWFManager.IsSync())
                                    cmd.ExecuteNonQuery();
                                else
                                    await cmd.ExecuteNonQueryAsync();
                            } catch (Exception exc) {
                                throw new InternalError($"{Path.GetFileName(file)} in package {package.Name}: {ErrorHandling.FormatExceptionMessage(exc)}");
                            }
                        }
                    }
                }
            }
        }

        private Regex reGo = new Regex(@"^\s*GO\s*$", RegexOptions.Compiled | RegexOptions.Multiline);

        /// <summary>
        /// Adds data for a new site.
        /// </summary>
        /// <remarks>
        /// When a new site is created, the AddSiteDataAsync method is called for all data providers.
        /// Data providers can then add site-specific data as the new site is added.</remarks>
        public Task AddSiteDataAsync() { return Task.CompletedTask; }
        /// <summary>
        /// Exports data from the data provider.
        /// </summary>
        /// <param name="chunk">The zero-based chunk number as data is exported. The first call when exporting begins specifies 0 as chunk number.</param>
        /// <param name="fileList">A collection of files. The data provider can add files to be exported to this collection when ExportChunkAsync is called.</param>
        /// <returns>Returns a YetaWF.Core.DataProvider.DataProviderExportChunk object describing the data exported.</returns>
        /// <remarks>
        /// The ExportChunkAsync method is called to export data for site backups, page and module exports.
        ///
        /// When a data provider is called to export data, it is called repeatedly until YetaWF.Core.DataProvider.DataProviderExportChunk.More is returned as false.
        /// Each time it is called, it is expected to export a chunk of data. The amount of data, i.e., the chunk size, is determined by the data provider.
        ///
        /// Each time ExportChunkAsync method is called, the zero-based chunk number <paramref name="chunk"/> is incremented.
        /// The data provider returns data in an instance of the YetaWF.Core.DataProvider.DataProviderExportChunk object.
        ///
        /// Files to be exported can be added to the <paramref name="fileList"/> collection.
        /// Only data records need to be added to the returned YetaWF.Core.DataProvider.DataProviderExportChunk object.
        /// </remarks>
        public Task<DataProviderExportChunk> ExportChunkAsync(int chunk, SerializableList<SerializableFile> fileList) { return Task.FromResult(new DataProviderExportChunk()); }
        /// <summary>
        /// Imports data into the data provider.
        /// </summary>
        /// <param name="chunk">The zero-based chunk number as data is imported. The first call when importing begins specifies 0 as chunk number.</param>
        /// <param name="fileList">A collection of files to be imported. Files are automatically imported, so the data provider doesn't have to process this collection.</param>
        /// <param name="obj">The data to be imported.</param>
        /// <remarks>
        /// The ImportChunkAsync method is called to import data for site restores, page and module imports.
        ///
        /// When a data provider is called to import data, it is called repeatedly until no more data is available.
        /// Each time it is called, it is expected to import the chunk of data defined by <paramref name="obj"/>.
        /// Each time ImportChunkAsync method is called, the zero-based chunk number <paramref name="chunk"/> is incremented.
        ///
        /// The <paramref name="obj"/> parameter is provided without type but should be cast to
        /// YetaWF.Core.Serializers.SerializableList&lt;OBJTYPE&gt; as it is a collection of records to import. All records in the collection must be imported.
        /// </remarks>
        public Task ImportChunkAsync(int chunk, SerializableList<SerializableFile> fileList, object obj) { return Task.CompletedTask; }

        /// <summary>
        /// Installs all data models (files, tables, etc.) for the data provider.
        /// </summary>
        /// <param name="errorList">A collection of error strings in user displayable format.</param>
        /// <returns>true if the models were created successfully, false otherwise.
        /// If the models could not be created, <paramref name="errorList"/> contains the reason for the failure.</returns>
        /// <remarks>
        /// While a package is installed, all data models are installed by calling the InstallModelAsync method.</remarks>
        public async Task<bool> InstallModelAsync(List<string> errorList) {
            await InitializeAsync(Package.GetPackageFromType(GetType()));
            return true;
        }
        /// <summary>
        /// Returns whether the data provider is installed and available.
        /// </summary>
        /// <returns>true if the data provider is installed and available, false otherwise.</returns>
        public Task<bool> IsInstalledAsync() { return Task.FromResult(true); }
        /// <summary>
        /// Called to translate the data managed by the data provider to another language.
        /// </summary>
        /// <param name="language">The target language (see LanguageSettings.json).</param>
        /// <param name="isHtml">A method that can be called by the data provider to test whether a string contains HTML.</param>
        /// <param name="translateStringsAsync">A method that can be called to translate a collection of simple strings into the new language. A simple string does not contain HTML or newline characters.</param>
        /// <param name="translateComplexStringAsync">A method that can be called to translate a collection of complex strings into the new language. A complex string can contain HTML and newline characters.</param>
        /// <remarks>
        /// The data provider has to retrieve all records and translate them as needed using the
        /// provided <paramref name="translateStringsAsync"/> and <paramref name="translateComplexStringAsync"/> methods, and save the translated data.
        ///
        /// The YetaWF.Core.Models.ObjectSupport.TranslateObject method can be used to translate all YetaWF.Core.Models.MultiString instances.
        ///
        /// The translated data should be stored separately from the default language (except MultiString, which is part of the record).
        /// Using the <paramref name="language"/> parameter, a different folder should be used to store the translated data.
        /// </remarks>
        public Task LocalizeModelAsync(string language, Func<string, bool> isHtml, Func<List<string>, Task<List<string>>> translateStringsAsync, Func<string, Task<string>> translateComplexStringAsync) { return Task.CompletedTask; }
        /// <summary>
        /// Removes data when a site is deleted.
        /// </summary>
        /// <remarks>
        /// When a site is deleted, the RemoveSiteDataAsync method is called for all data providers.
        /// Data providers can then remove site-specific data as the site is removed.</remarks>
        public Task RemoveSiteDataAsync() { return Task.CompletedTask; }
        /// <summary>
        /// Uninstalls all data models (files, tables, etc.) for the data provider.
        /// </summary>
        /// <param name="errorList">A collection of error strings in user displayable format.</param>
        /// <returns>true if the models were removed successfully, false otherwise.
        /// If the models could not be removed, <paramref name="errorList"/> contains the reason for the failure.</returns>
        /// <remarks>
        /// While a package is uninstalled, all data models are uninstalled by calling the UninstallModelAsync method.</remarks>
        public Task<bool> UninstallModelAsync(List<string> errorList) { return Task.FromResult(true); }
    }
}
