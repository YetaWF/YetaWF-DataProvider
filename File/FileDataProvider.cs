/* Copyright © 2019 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using YetaWF.Core;
using YetaWF.Core.Controllers;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.IO;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider {

    /// <summary>
    /// Provides an identity for records, similar to an identity in a SQL database record.
    /// The identity is a unique value describing a record in one dataset.
    /// </summary>
    public class FileIdentityCount {

        /// <summary>
        /// The initial identity value of the first record in a dataset.
        /// </summary>
        public const int IDENTITY_SEED = 1000;

        /// <summary>
        /// Constructor.
        /// </summary>
        public FileIdentityCount() { Count = IDENTITY_SEED; }
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="seed">The initial identity value of the first record in the dataset.</param>
        public FileIdentityCount(int seed) { Count = seed; }
        /// <summary>
        /// The last used identity value.
        /// This is incremented for every new record.
        /// </summary>
        public int Count { get; set; }
    }

    /// <summary>
    /// The base class for the file data provider.
    /// This base class is not intended for use by application data providers. These use one of the more specialized derived classes instead.
    /// </summary>
    public class FileDataProviderBase {
        /// <summary>
        /// The name of the file data provider. This name is used in appsettings.json to select the data provider.
        /// </summary>
        public static readonly string ExternalName = "File";
    }

    /// <summary>
    /// This template class implements record-based I/O using the installed file I/O provider and local/shared caching.
    /// The file data provider allows record-based I/O, similar to a SQL table and is a low-level data provider.
    /// It is used by application data providers and not by applications directly.
    /// The implementation details are hidden from the application and local/shared caching is used as appropriate, based on definitions in appsettings.json.
    ///
    /// The file data provider should only be used with very small datasets.
    ///
    /// Any class derived from FileDataProvider&lt;KEYTYPE, OBJTYPE&gt; is provided with a complete implementation
    /// of record-level I/O (browse, read, add, update, remove) accessing the defined dataset.
    /// </summary>
    /// <typeparam name="KEYTYPE">The type of the primary key property.</typeparam>
    /// <typeparam name="OBJTYPE">The type of the object (one record) in the dataset.</typeparam>
    public class FileDataProvider<KEYTYPE, OBJTYPE> : FileDataProviderBase, IDataProvider<KEYTYPE, OBJTYPE>, IDisposable, IDataProviderTransactions {

        /// <summary>
        /// A dictionary of options and optional parameters as provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider was created.
        /// </summary>
        public Dictionary<string, object> Options { get; private set; }
        /// <summary>
        /// The package implementing the data provider.
        /// </summary>
        public Package Package { get; private set; }
        /// <summary>
        /// The dataset provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider was created.
        /// </summary>
        public string Dataset { get; protected set; }
        /// <summary>
        /// The full path of the folder where the data provider stores its data.
        /// The FileDataProvider.GetBaseFolder() method is used to define the full path.
        /// </summary>
        public string BaseFolder { get; private set; }
        /// <summary>
        /// The site identity provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider was created.
        ///
        /// This may be 0 if no specific site is associated with the data provider.
        /// </summary>
        public int SiteIdentity { get; private set; }
        /// <summary>
        /// Defines whether the site identity {i}SiteIdentity{/i} was provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider was created.
        /// </summary>
        public bool UseIdentity { get; private set; }
        /// <summary>
        /// The initial value of the identity seed. The default value is defined by FileIdentityCount.IDENTITY_SEED, but this can be overridden by passing an
        /// optional IdentitySeed parameter to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.
        /// </summary>
        public int IdentitySeed { get; private set; }
        /// <summary>
        /// Defines whether the data is cacheable.
        /// This corresponds to the Cacheable parameter of the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method.
        /// </summary>
        public bool Cacheable { get; private set; }
        /// <summary>
        /// Defines whether logging is wanted for the data provider. The default value is false, but this can be overridden by passing an
        /// optional Logging parameter to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.
        ///
        /// This does not appear to be used at this time by this data provider.
        /// </summary>
        public bool Logging { get; private set; }

        /// <summary>
        /// An optional callback which is called whenever an object is retrieved to update some properties.
        /// </summary>
        /// <remarks>
        /// Properties that are derived from other property values are considered "calculated properties". This callback
        /// is called after retrieving an object to update these properties.
        ///
        /// This callback is typically set by the data provider itself, in its constructor or as the data provider is being created.
        /// </remarks>
        public Func<string, object, Task<object>> CalculatedPropertyCallbackAsync { get; set; }

        private const int ChunkSize = 100;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="options">A collection of all settings and optional parameters provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider was created.</param>
        /// <remarks>
        /// For debugging purposes, instances of this class are tracked using the DisposableTracker class.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2214:DoNotCallOverridableMethodsInConstructors")]
        public FileDataProvider(Dictionary<string, object> options) {
            Options = options;
            if (!Options.ContainsKey("Package") || !(Options["Package"] is Package))
                throw new InternalError($"No Package for data provider {GetType().FullName}");
            Package = (Package)Options["Package"];
            if (!Options.ContainsKey("Dataset") || !(Options["Dataset"] is string))
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

            UseIdentity = !string.IsNullOrWhiteSpace(IdentityName);

            this.IdentitySeed = IdentitySeed == 0 ? FileIdentityCount.IDENTITY_SEED : IdentitySeed;

            BaseFolder = GetBaseFolder();

            DisposableTracker.AddObject(this);
        }

        /// <summary>
        /// Returns the full path of the folder where this data provider stores its data.
        /// </summary>
        /// <returns>Returns the full path of the folder where this data provider stores its data.</returns>
        /// <remarks>Implementors of data providers, i.e. classes deriving from the class FileDataProvider&lt;KEYTYPE, OBJTYPE&gt; must override this method to
        /// provide a valid path.
        ///
        /// If a data provider is site dependent, the site identity should be part of the path name.
        /// </remarks>
        public virtual string GetBaseFolder() { throw new InternalError($"Override GetBaseFolder() in {GetType().FullName}"); }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose() { Dispose(true); }
        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="disposing">true to release the DisposableTracker reference count, false otherwise.</param>
        protected virtual void Dispose(bool disposing) { if (disposing) DisposableTracker.RemoveObject(this); }

        /// <summary>
        /// Returns the name of the property that represents the primary key column.
        /// </summary>
        /// <remarks>
        /// A property can be defined as the primary key column by using the YetaWF.Core.DataProvider.Attributes.Data_PrimaryKey attribute.
        ///
        /// A dataset must have a primary key.
        /// </remarks>
        public string Key1Name { get { return GetKey1Name(); } }

        /// <summary>
        /// Returns the name of the property that represents the identity column.
        /// </summary>
        /// <remarks>
        /// A property can be defined as the identity column by using the YetaWF.Core.DataProvider.Attributes.Data_Identity attribute.
        ///
        /// If no identity column is defined, this property return null.
        /// </remarks>
        public string IdentityName { get { return GetIdentityName(); } }

        /// <summary>
        /// Starts a transaction that can be committed, saving all updates, or aborted to abandon all updates.
        /// </summary>
        /// <returns>Returns a YetaWF.Core.DataProvider.DataProviderTransaction object.</returns>
        /// <remarks>This method is defined for symmetry with other data providers, but file data providers do not support transactions.</remarks>
        public DataProviderTransaction StartTransaction() {
            throw new NotSupportedException($"{nameof(StartTransaction)} is not supported");
        }
        /// <summary>
        /// Commits a transaction, saving all updates.
        /// </summary>
        /// <remarks>This method is defined for symmetry with other data providers, but file data providers do not support transactions.</remarks>
        public Task CommitTransactionAsync() {
            throw new NotSupportedException($"{nameof(CommitTransactionAsync)} is not supported");
        }
        /// <summary>
        /// Aborts a transaction, abandoning all updates.
        /// </summary>
        /// <remarks>This method is defined for symmetry with other data providers, but file data providers do not support transactions.</remarks>
        public void AbortTransaction() {
            throw new NotSupportedException($"{nameof(AbortTransaction)} is not supported");
        }

        private string GetKey1Name() {
            if (_key1Name == null) {
                // find primary key
                foreach (var prop in ObjectSupport.GetPropertyData(typeof(OBJTYPE))) {
                    if (prop.HasAttribute(Data_PrimaryKey.AttributeName)) {
                        _key1Name = prop.Name;
                        return prop.Name;
                    }
                }
                throw new InternalError("Primary key not defined in {0}", typeof(OBJTYPE).FullName);
            }
            return _key1Name;
        }
        private string _key1Name;

        private string GetIdentityName() {
            if (_identityName == null) {
                // find identity
                foreach (var prop in ObjectSupport.GetPropertyData(typeof(OBJTYPE))) {
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

        private const string InternalFilePrefix = "__";

        private FileData<OBJTYPE> GetFileDataObject(KEYTYPE key) {
            //if (typeof(KEYTYPE) == typeof(string))
            //    key = (KEYTYPE) (object) FileData.MakeValidFileName((string) (object) key);

            //string fullPath = Path.Combine(BaseFolder, key.ToString());
            //string baseFolder = Path.GetDirectoryName(fullPath);
            //string fileName = Path.GetFileName(fullPath);

            FileData<OBJTYPE> fd = new FileData<OBJTYPE> {
                BaseFolder = BaseFolder,
                FileName = key.ToString().ToLower(),
                Cacheable = Cacheable
            };
            return fd;
        }
        /// <summary>
        /// Retrieves the record with the specified primary key.
        /// </summary>
        /// <param name="key">The primary key.</param>
        /// <returns>Returns the record with the specified primary key, or null if there is no record with the specified primary key.
        /// Other errors cause an exception.</returns>
        public async Task<OBJTYPE> GetAsync(KEYTYPE key) {
            return await GetAsync(key, SpecificTypeOnly: false);
        }
        private async Task<OBJTYPE> GetSpecificTypeAsync(KEYTYPE key) {
            return await GetAsync(key, SpecificTypeOnly: true);
        }
        private async Task<OBJTYPE> GetAsync(KEYTYPE key, bool SpecificTypeOnly) {
            FileData<OBJTYPE> fd = GetFileDataObject(key);
            OBJTYPE o = await fd.LoadAsync(SpecificTypeOnly: SpecificTypeOnly);
            if (o == null) return default(OBJTYPE);
            return await UpdateCalculatedPropertiesAsync(o);
        }
        /// <summary>
        /// Adds a new record.
        /// </summary>
        /// <param name="obj">The new record.</param>
        /// <returns>Returns true if the record was successfully added, or false if the primary key already exists.
        /// Other errors cause an exception.</returns>
        public async Task<bool> AddAsync(OBJTYPE obj) {

            PropertyInfo piKey = ObjectSupport.GetProperty(typeof(OBJTYPE), Key1Name);
            KEYTYPE key = (KEYTYPE)piKey.GetValue(obj);

            if (!string.IsNullOrWhiteSpace(IdentityName)) {
                // using identity
                int identity = 0;
                PropertyInfo piIdent = ObjectSupport.GetProperty(typeof(OBJTYPE), IdentityName);
                if (piIdent == null) throw new InternalError("Type {0} has no identity property named {1}", typeof(OBJTYPE).FullName, IdentityName);
                if (piIdent.PropertyType != typeof(int)) throw new InternalError("FileDataProvider only supports object identities of type int");

                FileData<FileIdentityCount> fdIdent = new FileData<FileIdentityCount> {
                    BaseFolder = BaseFolder,
                    FileName = InternalFilePrefix + IdentityName,
                };
                using (ILockObject lockObject = await YetaWF.Core.IO.Caching.LockProvider.LockResourceAsync($"{AreaRegistration.CurrentPackage.AreaName}_FileDataProvider_{BaseFolder}")) {
                    FileIdentityCount ident = await fdIdent.LoadAsync();
                    if (ident == null) { // new
                        ident = new FileIdentityCount(IdentitySeed);
                        await fdIdent.AddAsync(ident);
                    } else { // existing
                        ++ident.Count;
                        await fdIdent.UpdateFileAsync(fdIdent.FileName, ident);
                    }
                    identity = ident.Count;
                    await lockObject.UnlockAsync();
                }
                piIdent.SetValue(obj, identity);
                if (Key1Name == IdentityName)
                    key = (KEYTYPE)(object)identity;
            }
            FileData<OBJTYPE> fd = GetFileDataObject(key);
            return await fd.AddAsync(obj);
        }
        /// <summary>
        /// Updates an existing record with the specified primary key.
        /// </summary>
        /// <param name="origKey">The original primary key value of the record.</param>
        /// <param name="newKey">The new primary key value of the record. This may be the same value as <paramref name="origKey"/>. </param>
        /// <param name="obj">The record data with the new information.</param>
        /// <returns>Returns a status indicator.</returns>
        public Task<UpdateStatusEnum> UpdateAsync(KEYTYPE origKey, KEYTYPE newKey, OBJTYPE obj) {
            return UpdateFileAsync(origKey, newKey, obj);
        }
        private async Task<UpdateStatusEnum> UpdateFileAsync(KEYTYPE origKey, KEYTYPE newKey, OBJTYPE obj) {
            FileData<OBJTYPE> fd = GetFileDataObject(origKey);
            return await fd.UpdateFileAsync(newKey.ToString().ToLower(), obj);
        }
        /// <summary>
        /// Removes an existing record with the specified primary key.
        /// </summary>
        /// <param name="key">The primary key value of the record to remove.</param>
        /// <returns>Returns true if the record was removed, or false if the record was not found. Other errors cause an exception.</returns>
        public async Task<bool> RemoveAsync(KEYTYPE key) {
            FileData<OBJTYPE> fd = GetFileDataObject(key);
            return await fd.TryRemoveAsync();
        }
        /// <summary>
        /// Given a base folder, returns a collection of primary key values for all records in the dataset.
        /// </summary>
        /// <param name="baseFolder">The full path of the folder.</param>
        /// <returns>Returns a collection of primary key values for all records in the dataset.</returns>
        public async Task<List<KEYTYPE>> GetListOfKeysAsync(string baseFolder) {
            List<string> files = await DataFilesProvider.GetDataFileNamesAsync(baseFolder);
            files = (from string f in files where !f.StartsWith(InternalFilePrefix) && f != Globals.DontDeployMarker select f).ToList<string>();

            if (typeof(KEYTYPE) == typeof(string))
                return (List<KEYTYPE>)(object)files;
            else if (typeof(KEYTYPE) == typeof(Guid))
                return (from string f in files select (KEYTYPE)(object)new Guid(f)).ToList<KEYTYPE>();
            else
                throw new InternalError("FileDataProvider only supports object keys of type string or Guid");
        }

        // GETRECORDS
        // GETRECORDS
        // GETRECORDS

        /// <summary>
        /// Retrieves one record using filtering criteria.
        /// </summary>
        /// <param name="filters">A collection describing the filtering criteria.</param>
        /// <param name="Joins">A collection describing the dataset joins. Not supported by file data providers. Must be null for file data providers.</param>
        /// <returns>If more than one record match the filtering criteria, the first one is returned.
        /// If no record matches, null is returned.</returns>
        /// <remarks>
        /// </remarks>
        public async Task<OBJTYPE> GetOneRecordAsync(List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            if (Joins != null) throw new InternalError("Joins not supported");
            DataProviderGetRecords<OBJTYPE> objs = await GetRecords(0, 1, null, filters);
            return await UpdateCalculatedPropertiesAsync(objs.Data.FirstOrDefault());
        }
        /// <summary>
        /// Retrieves a collection of records using filtering criteria with sorting, with support for paging.
        /// </summary>
        /// <param name="skip">The number of records to skip (paging support).</param>
        /// <param name="take">The number of records to retrieve (paging support). If more records are available they are dropped.</param>
        /// <param name="sort">A collection describing the sort order.</param>
        /// <param name="filters">A collection describing the filtering criteria.</param>
        /// <param name="Joins">A collection describing the dataset joins. Not supported by file data providers. Must be null for file data providers.</param>
        /// <returns>Returns a YetaWF.Core.DataProvider.DataProviderGetRecords object describing the data returned.</returns>
        public Task<DataProviderGetRecords<OBJTYPE>> GetRecordsAsync(int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            return GetRecords(skip, take, sort, filters, Joins: Joins, SpecificTypeOnly: false);
        }
        private Task<DataProviderGetRecords<OBJTYPE>> GetRecordsSpecificTypeAsync(int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            return GetRecords(skip, take, sort, filters, Joins: Joins, SpecificTypeOnly: true);
        }
        private async Task<DataProviderGetRecords<OBJTYPE>> GetRecords(int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, List<JoinData> Joins = null, bool SpecificTypeOnly = false) {

            if (Joins != null) throw new InternalError("Joins not supported");
            List<string> files = await DataFilesProvider.GetDataFileNamesAsync(BaseFolder);

            List<OBJTYPE> objects = new List<OBJTYPE>();

            foreach (string file in files) {

                if (file.StartsWith(InternalFilePrefix) || file == Globals.DontDeployMarker) // internal file
                    continue;

                KEYTYPE key;
                if (typeof(KEYTYPE) == typeof(string))
                    key = (KEYTYPE)(object)file;
                else if (typeof(KEYTYPE) == typeof(Guid))
                    key = (KEYTYPE)(object)new Guid(file);
                else if (typeof(KEYTYPE) == typeof(int))
                    key = (KEYTYPE)(object)Convert.ToInt32(file);
                else
                    throw new InternalError("FileDataProvider only supports object keys of type string, int or Guid");

                OBJTYPE obj;
                if (SpecificTypeOnly) {
                    obj = await GetSpecificTypeAsync(key);
                    if (obj == null || typeof(OBJTYPE) != obj.GetType())
                        continue;
                } else {
                    obj = await GetAsync(key);
                    if (obj == null)
                        throw new InternalError($"Object in file {file} is invalid");
                }
                objects.Add(obj);

                if (skip == 0 && sort == null && filters == null) {
                    if (objects.Count == take)
                        break;
                }
            }
            foreach (OBJTYPE obj in objects)
                await UpdateCalculatedPropertiesAsync(obj);
            objects = DataProviderImpl<OBJTYPE>.Filter(objects, filters);
            int total = objects.Count;
            objects = DataProviderImpl<OBJTYPE>.Sort(objects, sort);

            if (skip > 0)
                objects = objects.Skip(skip).ToList<OBJTYPE>();
            if (take > 0)
                objects = objects.Take(take).ToList<OBJTYPE>();
            return new DataProviderGetRecords<OBJTYPE> {
                Data = objects,
                Total = total,
            };
        }
        private async Task<OBJTYPE> UpdateCalculatedPropertiesAsync(OBJTYPE obj) {
            if (CalculatedPropertyCallbackAsync == null) return obj;
            List<PropertyData> props = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
            props = (from p in props where p.CalculatedProperty select p).ToList();
            foreach (PropertyData prop in props) {
                obj = (OBJTYPE)await CalculatedPropertyCallbackAsync(prop.Name, obj);
            }
            return obj;
        }

        // REMOVE RECORDS
        // REMOVE RECORDS
        // REMOVE RECORDS

        /// <summary>
        /// Removes records using filtering criteria.
        /// </summary>
        /// <param name="filters">A collection describing the filtering criteria.</param>
        /// <returns>Returns the number of records removed.</returns>
        public async Task<int> RemoveRecordsAsync(List<DataProviderFilterInfo> filters) {
            List<string> files = await DataFilesProvider.GetDataFileNamesAsync(BaseFolder);

            int total = 0;
            foreach (string file in files) {
                if (file.StartsWith(InternalFilePrefix) || file == Globals.DontDeployMarker) // internal file
                    continue;
                KEYTYPE key;
                if (typeof(KEYTYPE) == typeof(string))
                    key = (KEYTYPE)(object)file;
                else if (typeof(KEYTYPE) == typeof(Guid))
                    key = (KEYTYPE)(object)new Guid(file);
                else if (typeof(KEYTYPE) == typeof(int))
                    key = (KEYTYPE)(object)Convert.ToInt32(file);
                else
                    throw new InternalError("FileDataProvider only supports object keys of type string, int or Guid");
                OBJTYPE obj = await GetAsync(key);
                if (obj == null)
                    throw new InternalError("Object in file {0} is invalid", file);

                if (DataProviderImpl<OBJTYPE>.Filter(new List<OBJTYPE> { obj }, filters).Count > 0) {
                    FileData<OBJTYPE> fdtemp = GetFileDataObject(key);
                    if (await fdtemp.TryRemoveAsync())
                        total++;
                }
            }
            if (filters == null)
                await RemoveFolderIfEmptyAsync(BaseFolder);
            return total;
        }

        private async Task RemoveFolderIfEmptyAsync(string path) {
            // delete the folder if it's empty now
            if ((await FileSystem.FileSystemProvider.GetDirectoriesAsync(path)).Count == 0) {
                List<string> files = await FileSystem.FileSystemProvider.GetFilesAsync(path);
                bool empty = true;
                foreach (string file in files) {
                    if (!Path.GetFileName(file).StartsWith(InternalFilePrefix) && file != Globals.DontDeployMarker) {// internal file
                        empty = false;
                        break;
                    }
                }
                if (empty) {
                    await FileSystem.FileSystemProvider.DeleteDirectoryAsync(BaseFolder);
                }
            }
        }

        // INSTALL/UNINSTALL
        // INSTALL/UNINSTALL
        // INSTALL/UNINSTALL

        /// <summary>
        /// Returns whether the data provider is installed and available.
        /// </summary>
        /// <returns>true if the data provider is installed and available, false otherwise.</returns>
        public async Task<bool> IsInstalledAsync() {
            return await FileSystem.FileSystemProvider.DirectoryExistsAsync(BaseFolder);
        }

        /// <summary>
        /// Installs all data models (files, tables, etc.) for the data provider.
        /// </summary>
        /// <param name="errorList">A collection of error strings in user displayable format.</param>
        /// <returns>true if the models were created successfully, false otherwise.
        /// If the models could not be created, <paramref name="errorList"/> contains the reason for the failure.</returns>
        /// <remarks>
        /// While a package is installed, all data models are installed by calling the InstallModelAsync method.</remarks>
        public async Task<bool> InstallModelAsync(List<string> errorList) {
            try {
                if (!await FileSystem.FileSystemProvider.DirectoryExistsAsync(BaseFolder))
                    await FileSystem.FileSystemProvider.CreateDirectoryAsync(BaseFolder);
                return true;
            } catch (Exception exc) {
                errorList.Add(string.Format("{0}: {1}", BaseFolder, ErrorHandling.FormatExceptionMessage(exc)));
                return false;
            }
        }
        /// <summary>
        /// Uninstalls all data models (files, tables, etc.) for the data provider.
        /// </summary>
        /// <param name="errorList">A collection of error strings in user displayable format.</param>
        /// <returns>true if the models were removed successfully, false otherwise.
        /// If the models could not be removed, <paramref name="errorList"/> contains the reason for the failure.</returns>
        /// <remarks>
        /// While a package is uninstalled, all data models are uninstalled by calling the UninstallModelAsync method.</remarks>
        public async Task<bool> UninstallModelAsync(List<string> errorList) {
            try {
                await DataFilesProvider.RemoveAllDataFilesAsync(BaseFolder);
                return true;
            } catch (Exception exc) {
                errorList.Add(string.Format("{0}: {1}", BaseFolder, ErrorHandling.FormatExceptionMessage(exc)));
                return false;
            }
        }
        /// <summary>
        /// Adds data for a new site.
        /// </summary>
        /// <remarks>
        /// When a new site is created the AddSiteDataAsync method is called for all data providers.
        /// Data providers can then add site-specific data as the new site is added.</remarks>
        public Task AddSiteDataAsync() { return Task.CompletedTask; }
        /// <summary>
        /// Removes data when a site is deleted.
        /// </summary>
        /// <remarks>
        /// When a site is deleted the RemoveSiteDataAsync method is called for all data providers.
        /// Data providers can then remove site-specific data as the site is removed.</remarks>
        public Task RemoveSiteDataAsync() { return Task.CompletedTask; } // remove site-specific data is performed globally by removing the site data folder

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
        public async Task<DataProviderExportChunk> ExportChunkAsync(int chunk, SerializableList<SerializableFile> fileList) {
            DataProviderGetRecords<OBJTYPE> recs = await GetRecordsSpecificTypeAsync(chunk * ChunkSize, ChunkSize, null, null);
            SerializableList<OBJTYPE> serList = new SerializableList<OBJTYPE>(recs.Data);
            object obj = serList;
            int count = serList.Count();
            if (count == 0)
                obj = null;
            return new DataProviderExportChunk {
                ObjectList = obj,
                More = count >= ChunkSize,
            };
        }
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
        public async Task ImportChunkAsync(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            if (SiteIdentity > 0 || YetaWFManager.Manager.ImportChunksNonSiteSpecifics) {
                SerializableList<OBJTYPE> serList = (SerializableList<OBJTYPE>)obj;
                int total = serList.Count();
                if (total > 0) {
                    for (int processed = 0; processed < total; ++processed) {
                        OBJTYPE item = serList[processed];
                        await AddAsync(item);
                    }
                }
            }
        }
        /// <summary>
        /// Called to translate the data managed by the data provider to another language.
        /// </summary>
        /// <param name="language">The target language (see LanguageSettings.json).</param>
        /// <param name="isHtml">A function that can be called by the data provider to test whether a string contains HTML.</param>
        /// <param name="translateStringsAsync">A method that can be called to translate a collection of simple strings into the new language. A simple string does not contain HTML or newline characters.</param>
        /// <param name="translateComplexStringAsync">A method that can be called to translate a collection of complex strings into the new language. A complex string can contain HTML and newline characters.</param>
        /// <remarks>
        /// The data provider has to retrieve all records and translate them as needed using the
        /// provided <paramref name="translateStringsAsync"/> and <paramref name="translateComplexStringAsync"/> methods, and save the translated data.
        ///
        /// The YetaWF.Core.Models.ObjectSupport.TranslateObject method can be used to translate all YetaWF.Core.Models.MultiString instances.
        ///
        /// The translated data should be stored separately from the default language (except YetaWF.Core.Models.MultiString, which is part of the record).
        /// Using the <paramref name="language"/> parameter, a different folder should be used to store the translated data.
        /// </remarks>
        public async Task LocalizeModelAsync(string language,
                Func<string, bool> isHtml,
                Func<List<string>, Task<List<string>>> translateStringsAsync, Func<string, Task<string>> translateComplexStringAsync) {

            List<PropertyInfo> props = ObjectSupport.GetProperties(typeof(OBJTYPE));
            PropertyInfo key1Prop = ObjectSupport.GetProperty(typeof(OBJTYPE), Key1Name);

            DataProviderGetRecords<OBJTYPE> data = await GetRecordsAsync(0, 0, null, null);
            foreach (OBJTYPE record in data.Data) {
                bool changed = await ObjectSupport.TranslateObject(record, language, isHtml, translateStringsAsync, translateComplexStringAsync, props);
                if (changed) {
                    KEYTYPE key1 = (KEYTYPE)key1Prop.GetValue(record);
                    UpdateStatusEnum status = await UpdateAsync(key1, key1, record);
                    if (status != UpdateStatusEnum.OK)
                        throw new InternalError($"Update failed for type {typeof(OBJTYPE).FullName} ({status})");
                }
            }
        }
    }
}
