/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using YetaWF.Core.Audit;
using YetaWF.Core.DataProvider;
using YetaWF.Core.IO;
using YetaWF.Core.Modules;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider {

    /// <summary>
    /// This class is used to install the required module repository support during application startup, by setting properties in the static class YetaWF.Core.IO.Module.
    /// </summary>
    /// <remarks>This class should not be instantiated and has no callable methods.</remarks>
    public class GenericModuleDefinitionDataProviderImpl : IInitializeApplicationStartup {

        // STARTUP
        // STARTUP
        // STARTUP

        /// <summary>
        /// Called during application startup.
        ///
        /// Installs all required methods to load/save/retrieve modules.
        /// </summary>
        public Task InitializeApplicationStartupAsync() {
            Module.LoadModuleDefinitionAsync = LoadModuleDefinitionAsync;
            Module.SaveModuleDefinitionAsync = SaveModuleDefinitionAsync;
            Module.RemoveModuleDefinitionAsync = RemoveModuleDefinitionAsync;
            DesignedModules.LoadDesignedModulesAsync = LoadDesignedModulesAsync;
            Module.GetModulesAsync = GetModulesAsync;
            Module.LockModuleAsync = LockModuleAsync;
            return Task.CompletedTask;
        }

        // CACHE
        // CACHE
        // CACHE

        private string CacheKey(Guid guid) {
            return string.Format("__Mod_{0}_{1}", YetaWFManager.Manager.CurrentSite.Identity, guid);
        }
        private class GetCachedModuleInfo {
            public ModuleDefinition Module { get; set; } = null!;
            public bool Success { get; set; }
        }
        private async Task<GetCachedModuleInfo> GetCachedModuleAsync(Guid guid) {
            GetCachedModuleInfo modInfo = new GetCachedModuleInfo();
            GetObjectInfo<ModuleDefinition> objInfo;
            using (ICacheDataProvider sharedCacheDP = YetaWF.Core.IO.Caching.GetSharedCacheProvider()) {
                objInfo = await sharedCacheDP.GetAsync<ModuleDefinition>(CacheKey(guid));
            }
            if (!objInfo.Success)
                return modInfo;
            modInfo.Success = true;
            modInfo.Module = objInfo.RequiredData;
            return modInfo;
        }
        private async Task SetCachedModuleAsync(ModuleDefinition mod) {
            using (ICacheDataProvider sharedCacheDP = YetaWF.Core.IO.Caching.GetSharedCacheProvider()) {
                await sharedCacheDP.AddAsync(CacheKey(mod.ModuleGuid), mod);
            }
        }
        private async Task SetEmptyCachedModuleAsync(Guid guid) {
            using (ICacheDataProvider sharedCacheDP = YetaWF.Core.IO.Caching.GetSharedCacheProvider()) {
                await sharedCacheDP.AddAsync<ModuleDefinition>(CacheKey(guid), null);
            }
        }
        private async Task RemoveCachedModuleAsync(Guid guid) {
            using (ICacheDataProvider sharedCacheDP = YetaWF.Core.IO.Caching.GetSharedCacheProvider()) {
                await sharedCacheDP.RemoveAsync<ModuleDefinition>(CacheKey(guid));
            }
        }

        // Implementation
        // Implementation
        // Implementation

        private async Task<SerializableList<DesignedModule>> LoadDesignedModulesAsync() {
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                using (ILockObject lockObject = await modDP.LockDesignedModulesAsync()) {
                    return await modDP.LoadDesignedModulesAsync();
                }
            }
        }
        private async Task GetModulesAsync(Module.ModuleBrowseInfo info) {
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                DataProviderGetRecords<ModuleDefinition> recs = await modDP.GetModulesAsync(info.Skip, info.Take, info.Sort, info.Filters);
                info.Modules = recs.Data;
                info.Total = recs.Total;
            }
        }
        private async Task<ModuleDefinition?> LoadModuleDefinitionAsync(Guid guid) {
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                GetCachedModuleInfo modInfo = await GetCachedModuleAsync(guid);
                ModuleDefinition? mod;
                if (modInfo.Success) {
                    mod = modInfo.Module;
                } else {
                    mod = await modDP.LoadModuleDefinitionAsync(guid);
                    if (mod != null)
                        await SetCachedModuleAsync(mod);
                    else
                        await SetEmptyCachedModuleAsync(guid);
                }
                return mod;
            }
        }

        private async Task SaveModuleDefinitionAsync(ModuleDefinition mod, IModuleDefinitionIO dataProvider) {
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                using (ILockObject lockObject = await modDP.LockDesignedModulesAsync()) {
                    await dataProvider.SaveModuleDefinitionAsync(mod);
                    await SetCachedModuleAsync(mod);
                    await lockObject.UnlockAsync();
                }
            }
        }

        private async Task<bool> RemoveModuleDefinitionAsync(Guid guid) {
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                using (ILockObject lockObject = await modDP.LockDesignedModulesAsync()) {
                    await RemoveCachedModuleAsync(guid);
                    bool ret = await modDP.RemoveModuleDefinitionAsync(guid);
                    await lockObject.UnlockAsync();
                    return ret;
                }
            }
        }

        private string MODULEKEY = $"__Module__{YetaWFManager.Manager.CurrentSite.Identity}__";

        private async Task<ILockObject> LockModuleAsync(Guid moduleGuid) {
            return await YetaWF.Core.IO.Caching.LockProvider.LockResourceAsync(MODULEKEY + moduleGuid.ToString());
        }
    }

    internal interface ModuleDefinitionDataProviderIOMode {
        Task<SerializableList<DesignedModule>> GetDesignedModulesAsync();
    }

    /// <summary>
    /// This class implements module retrieval and storage for all modules.
    /// All modules are derived from YetaWF.Core.Modules.ModuleDefinition so this data provider can retrieve all module types, however it only returns
    /// an instance of YetaWF.Core.Modules.ModuleDefinition and not the more specific derived type.
    /// </summary>
    /// <remarks>
    /// When using a SQL database, all modules use the same database and base table named YetaWF_Modules. In addition, for each derived type an additional table is created.
    /// The table name is derived from the module type. For example, a module of type YetaWF.Text.TextModule uses the table YetaWF_Modules_YetaWF_Text_TextModule for all
    /// its data (except for the base data defined by its base class YetaWF.Core.Modules.ModuleDefinition, which is stored in the base table YetaWF_Modules).
    ///
    /// When using file I/O for module storage, all module data is stored in folder .\Data\DataFolder\YetaWF_Modules\..siteidentity.. Base and derived data is stored in the same file.
    /// </remarks>
    internal class GenericModuleDefinitionDataProvider : ModuleDefinitionDataProvider<Guid, ModuleDefinition> { }

    /// <summary>
    /// This template class implements module retrieval and storage for a specific module type.
    ///
    /// Every module type implements its own module data provider using this template class.
    /// </summary>
    /// <typeparam name="KEY">The type of the key for records managed by this data provider. For module data providers this is always System.Guid.</typeparam>
    /// <typeparam name="TYPE">The module type, which is always a class derived from YetaWF.Core.Modules.ModuleDefinition.</typeparam>
    /// <remarks>None of the methods in this class should be called directly by applications.</remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1063:ImplementIDisposableCorrectly")]
    public class ModuleDefinitionDataProvider<KEY, TYPE> : DataProviderImpl, IModuleDefinitionIO, IInstallableModel where KEY : notnull where TYPE : notnull {

        // IMPLEMENTATION
        // IMPLEMENTATION
        // IMPLEMENTATION

        /// <summary>
        /// Constructor.
        /// </summary>
        public ModuleDefinitionDataProvider() : base(YetaWFManager.Manager.CurrentSite.Identity) { SetDataProvider(CreateDataProvider()); }

        private IDataProvider<KEY, TYPE> DataProvider { get { return GetDataProvider(); } }
        private ModuleDefinitionDataProviderIOMode DataProviderIOMode { get { return GetDataProvider(); } }

        private IDataProvider<KEY, TYPE> CreateDataProvider() {
            Package package = YetaWF.Core.Packages.Package.GetPackageFromType(typeof(TYPE));
            return CreateDataProviderIOMode(package, ModuleDefinition.BaseFolderName, SiteIdentity: SiteIdentity, Cacheable: true,
                Callback: (ioMode, options) => {
                    switch (ioMode) {
                        case "sql": {
                                options.Add("WebConfigArea", ModuleDefinition.BaseFolderName);
                                return new SQL.SQLDataProvider.ModuleDefinitionDataProvider<KEY, TYPE>(options);
                            }
                        case "postgresql": {
                                options.Add("WebConfigArea", ModuleDefinition.BaseFolderName);
                                return new PostgreSQL.PostgreSQLDataProvider.ModuleDefinitionDataProvider<KEY, TYPE>(options);
                            }
                        case "file":
                            return new File.FileDataProvider.ModuleDefinitionDataProvider<KEY, TYPE>(options);
                        default:
                            throw new InternalError($"Unsupported IOMode {ioMode} in {nameof(ModuleDefinitionDataProvider<KEY, TYPE>)}.{nameof(CreateDataProvider)}");
                    }
                }
            );
        }

        // API
        // API
        // API

        /// <summary>
        /// Retrieves a collection of records using filtering criteria with sorting, with support for paging.
        /// </summary>
        /// <param name="skip">The number of records to skip (paging support).</param>
        /// <param name="take">The number of records to retrieve (paging support). If more records are available they are dropped.</param>
        /// <param name="sort">A collection describing the sort order.</param>
        /// <param name="filters">A collection describing the filtering criteria.</param>
        /// <returns>Returns a YetaWF.Core.DataProvider.DataProviderGetRecords object describing the data returned.</returns>
        internal async Task<DataProviderGetRecords<TYPE>> GetModulesAsync(int skip, int take, List<DataProviderSortInfo>? sort, List<DataProviderFilterInfo>? filters) {
            return await DataProvider.GetRecordsAsync(skip, take, sort, filters);
        }

        /// <summary>
        /// Loads the module.
        /// </summary>
        /// <param name="key">The module Guid.</param>
        /// <returns>Returns the YetaWF.Core.Modules.ModuleDefinition instance or null if module doesn't exist.
        /// If the template class is used with a specific derived module type, the returned instance can be cast to the more specific type.</returns>
        /// <remarks>This is never called directly. Always use YetaWF.Core.Module.ModuleDefinition.LoadModuleDefinitionAsync to load a module.</remarks>
        public async Task<ModuleDefinition?> LoadModuleDefinitionAsync(Guid key) {
            return (ModuleDefinition?)(object?)await DataProvider.GetAsync((KEY)(object)key);
        }

        /// <summary>
        /// Saves the module.
        /// </summary>
        /// <param name="mod">The module to save.</param>
        /// <remarks>This is never called directly. Always use YetaWF.Core.Module.ModuleDefinition.SaveModuleDefinitionAsync to save a module.</remarks>
        public async Task SaveModuleDefinitionAsync(ModuleDefinition mod) {

            Guid key = mod.ModuleGuid;

            ModuleDefinition? origMod = YetaWF.Core.Audit.Auditing.Active ? (ModuleDefinition?)(object?)await DataProvider.GetAsync((KEY)(object)key) : null;

            mod.DateUpdated = DateTime.UtcNow;
            await SaveImagesAsync(key, mod);
            await mod.ModuleSavingAsync();

            UpdateStatusEnum status = await DataProvider.UpdateAsync((KEY)(object)key, (KEY)(object)key, (TYPE)(object)mod);
            if (status != UpdateStatusEnum.OK)
                if (!await DataProvider.AddAsync((TYPE)(object)mod))
                    throw new InternalError("Can't add module definition for {0}", key);

            SerializableList<DesignedModule> designedModules = await GetDesignedModulesAsync();
            DesignedModule? desMod = (from d in designedModules where d.ModuleGuid == key select d).FirstOrDefault();
            if (desMod != null) {
                desMod.Name = mod.Name;
            } else {
                desMod = new DesignedModule() {
                    ModuleGuid = key,
                    Description = mod.Description,
                    Name = mod.Name,
                    AreaName = mod.AreaName,
                };
                designedModules.Add(desMod);
            }
            await SaveDesignedModulesAsync(designedModules);

            await Auditing.AddAuditAsync($"{nameof(ModuleDefinitionDataProvider<KEY, TYPE>)}.{nameof(SaveModuleDefinitionAsync)}", origMod?.Name, mod.ModuleGuid,
                "Save Module",
                DataBefore: origMod,
                DataAfter: mod,
                ExpensiveMultiInstance: true
            );
        }
        /// <summary>
        /// Removes the module.
        /// </summary>
        /// <param name="key">The module Guid of the module to remove.</param>
        /// <remarks>This is never called directly. Always use YetaWF.Core.Module.ModuleDefinition.RemoveModuleDefinitionAsync to remove a module.</remarks>
        internal async Task<bool> RemoveModuleDefinitionAsync(Guid key) {

            ModuleDefinition? mod = null;

            try {
                mod = await LoadModuleDefinitionAsync(key);
                if (mod != null)
                    await mod.ModuleRemovingAsync();
            } catch (Exception) { }

            bool status = await DataProvider.RemoveAsync((KEY)(object)key);
            if (status) {
                // remove the data folder (if any)
                string dir = ModuleDefinition.GetModuleDataFolder(key);
                await FileSystem.FileSystemProvider.DeleteDirectoryAsync(dir);
            }

            SerializableList<DesignedModule> designedModules = await GetDesignedModulesAsync();
            DesignedModule? desMod = (from d in designedModules where d.ModuleGuid == key select d).FirstOrDefault();
            if (desMod != null) {
                designedModules.Remove(desMod);
                await SaveDesignedModulesAsync(designedModules);
            }

            if (mod != null || desMod != null) {
                await Auditing.AddAuditAsync($"{nameof(ModuleDefinitionDataProvider<KEY, TYPE>)}.{nameof(SaveModuleDefinitionAsync)}", mod?.Name ?? desMod?.Name, mod?.ModuleGuid ?? desMod!.ModuleGuid,
                    "Remove Module",
                    DataBefore: mod,
                    DataAfter: null,
                    ExpensiveMultiInstance: true
                );
            }
            return status;
        }

        // DESIGNED MODULES
        // DESIGNED MODULES
        // DESIGNED MODULES

        // Designed modules are site specific and DesignedModules is a permanent site-specific object

        private string DESIGNEDMODULESKEY = $"__DesignedModules__{YetaWFManager.Manager.CurrentSite.Identity}";

        internal async Task<SerializableList<DesignedModule>> LoadDesignedModulesAsync() {
            SerializableList<DesignedModule> list = new SerializableList<DesignedModule>();
            if (await DataProvider.IsInstalledAsync()) {// a new site may not have the data installed yet
                list = await GetDesignedModulesAsync();
            }
            return list;
        }
        internal async Task<SerializableList<DesignedModule>> GetDesignedModulesAsync() {
            using (ICacheDataProvider staticCacheDP = YetaWF.Core.IO.Caching.GetStaticCacheProvider()) {
                SerializableList<DesignedModule> list;
                GetObjectInfo<SerializableList<DesignedModule>> info = await staticCacheDP.GetAsync<SerializableList<DesignedModule>>(DESIGNEDMODULESKEY);
                if (info.Success)
                    list = info.RequiredData;
                else {
                    list = await DataProviderIOMode.GetDesignedModulesAsync();
                    await staticCacheDP.AddAsync(DESIGNEDMODULESKEY, list);
                }
                return list;
            }
        }
        internal async Task SaveDesignedModulesAsync(SerializableList<DesignedModule> list) {
            using (ICacheDataProvider staticCacheDP = YetaWF.Core.IO.Caching.GetStaticCacheProvider()) {
                await staticCacheDP.AddAsync<SerializableList<DesignedModule>>(DESIGNEDMODULESKEY, list);
            }
        }
        internal async Task<ILockObject> LockDesignedModulesAsync() {
            return await YetaWF.Core.IO.Caching.LockProvider.LockResourceAsync(DESIGNEDMODULESKEY);
        }

        // IINSTALLABLEMODEL
        // IINSTALLABLEMODEL
        // IINSTALLABLEMODEL

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
        public async new Task<DataProviderExportChunk> ExportChunkAsync(int chunk, SerializableList<SerializableFile> fileList) {
            DataProviderExportChunk exp = await DataProvider.ExportChunkAsync(chunk, fileList);
            if (exp.ObjectList != null) {
                SerializableList<TYPE> modList = new SerializableList<TYPE>((List<TYPE>)exp.ObjectList);
                foreach (TYPE m in modList) {
                    ModuleDefinition mod = (ModuleDefinition)(object)m;
                    fileList.AddRange(await Package.ProcessAllFilesAsync(mod.ModuleDataFolder));
                }
            }
            return exp;
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
        public new async Task ImportChunkAsync(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            await DataProvider.ImportChunkAsync(chunk, fileList, obj);
        }
    }
}
