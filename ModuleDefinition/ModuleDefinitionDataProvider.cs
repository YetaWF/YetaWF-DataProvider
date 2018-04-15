/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using YetaWF.Core.Audit;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.IO;
using YetaWF.Core.Models;
using YetaWF.Core.Modules;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;
using YetaWF.Core.Support.Serializers;
#if MVC6
using Microsoft.Extensions.Caching.Memory;
#else
#endif

namespace YetaWF.DataProvider
{
    public class GenericModuleDefinitionDataProviderImpl : IInitializeApplicationStartup {

        // STARTUP
        // STARTUP
        // STARTUP

        public Task InitializeApplicationStartupAsync() {
            ModuleDefinition.LoadModuleDefinitionAsync = LoadModuleDefinitionAsync;
            ModuleDefinition.SaveModuleDefinitionAsync = SaveModuleDefinitionAsync;
            ModuleDefinition.RemoveModuleDefinitionAsync = RemoveModuleDefinitionAsync;
            DesignedModules.LoadDesignedModulesAsync = LoadDesignedModulesAsync;
            ModuleDefinition.GetModulesAsync = GetModulesAsync;
            ModuleDefinition.LockModuleAsync = LockModuleAsync;
            return Task.CompletedTask;
        }

        // CACHE
        // CACHE
        // CACHE

        private string CacheKey(Guid guid) {
            return string.Format("__Mod_{0}_{1}", YetaWFManager.Manager.CurrentSite.Identity, guid);
        }
        private class GetCachedModuleInfo {
            public ModuleDefinition Module { get; set; }
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
            modInfo.Module = objInfo.Data;
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

        private async Task<SerializableList<DesignedModule>> LoadDesignedModulesAsync() {
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                using (ILockObject lockObject = await modDP.LockDesignedModulesAsync()) {
                    return await modDP.LoadDesignedModulesAsync();
                }
            }
        }
        private async Task GetModulesAsync(ModuleDefinition.ModuleBrowseInfo info) {
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                DataProviderGetRecords<ModuleDefinition> recs = await modDP.GetModulesAsync(info.Skip, info.Take, info.Sort, info.Filters);
                info.Modules = recs.Data;
                info.Total = recs.Total;
            }
        }
        private async Task<ModuleDefinition> LoadModuleDefinitionAsync(Guid guid) {
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                GetCachedModuleInfo modInfo = await GetCachedModuleAsync(guid);
                ModuleDefinition mod;
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
                    using (dataProvider) {
                        await dataProvider.SaveModuleDefinitionAsync(mod);
                    }
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

    public interface ModuleDefinitionDataProviderIOMode {
        Task<SerializableList<DesignedModule>> GetDesignedModulesAsync();
    }
    public class TempDesignedModule {
        [Data_PrimaryKey]
        public Guid ModuleGuid { get; set; }
        public string Name { get; set; }
        public MultiString Description { get; set; }
        public string DerivedAssemblyName { get; set; }

        public TempDesignedModule() {
            Description = new MultiString();
        }
    }

    // Loads/saves any module and creates the appropriate module type
    internal class GenericModuleDefinitionDataProvider : ModuleDefinitionDataProvider<Guid, ModuleDefinition> { }

    // Loads/saves a specific module type
    public class ModuleDefinitionDataProvider<KEY, TYPE> : DataProviderImpl, IModuleDefinitionIO, IInstallableModel {

        // IMPLEMENTATION
        // IMPLEMENTATION
        // IMPLEMENTATION

        public ModuleDefinitionDataProvider() : base(YetaWFManager.Manager.CurrentSite.Identity) { SetDataProvider(CreateDataProvider()); }
        public ModuleDefinitionDataProvider(int siteIdentity) : base(siteIdentity) { SetDataProvider(CreateDataProvider()); }

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

        internal async Task<DataProviderGetRecords<TYPE>> GetModulesAsync(int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters) {
            return await DataProvider.GetRecordsAsync(skip, take, sort, filters);
        }

        /// <summary>
        /// Load the module definition
        /// </summary>
        /// <returns>ModuleDefinition or null if module doesn't exist</returns>
        public async Task<ModuleDefinition> LoadModuleDefinitionAsync(Guid key) {
            return (ModuleDefinition)(object)await DataProvider.GetAsync((KEY)(object)key);
        }

        /// <summary>
        /// Save the module definition
        /// </summary>
        public async Task SaveModuleDefinitionAsync(ModuleDefinition mod) {

            Guid key = mod.ModuleGuid;

            ModuleDefinition origMod = YetaWF.Core.Audit.Auditing.Active ? (ModuleDefinition)(object)await DataProvider.GetAsync((KEY)(object)key) : null;

            mod.DateUpdated = DateTime.UtcNow;
            await SaveImagesAsync(key, mod);
            await mod.ModuleSavingAsync();

            UpdateStatusEnum status = await DataProvider.UpdateAsync((KEY)(object)key, (KEY)(object)key, (TYPE)(object)mod);
            if (status != UpdateStatusEnum.OK)
                if (!await DataProvider.AddAsync((TYPE)(object)mod))
                    throw new InternalError("Can't add module definition for {0}", key);

            SerializableList<DesignedModule> designedModules = await GetDesignedModulesAsync();
            DesignedModule desMod = (from d in designedModules where d.ModuleGuid == key select d).FirstOrDefault();
            if (desMod != null) {
                desMod.Name = mod.Name;
            } else {
                desMod = new DesignedModule() {
                    ModuleGuid = key,
                    Description = mod.Description,
                    Name = mod.Name,
                    AreaName = mod.Area,
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
        internal async Task<bool> RemoveModuleDefinitionAsync(Guid key) {
            bool status = false;
            ModuleDefinition mod = null;

            try {
                mod = await LoadModuleDefinitionAsync(key);
                if (mod != null)
                    await mod.ModuleRemovingAsync();
            } catch (Exception) { }

            SerializableList<DesignedModule> designedModules = await GetDesignedModulesAsync();
            DesignedModule desMod = (from d in designedModules where d.ModuleGuid == key select d).FirstOrDefault();
            if (desMod == null)
                status = false;
            else {
                designedModules.Remove(desMod);
                status = await DataProvider.RemoveAsync((KEY)(object)key);
            }
            if (status) {
                // remove the data folder (if any)
                string dir = ModuleDefinition.GetModuleDataFolder(key);
                await FileSystem.FileSystemProvider.DeleteDirectoryAsync(dir);
            }
            await SaveDesignedModulesAsync(designedModules);

            if (mod != null) {
                await Auditing.AddAuditAsync($"{nameof(ModuleDefinitionDataProvider<KEY, TYPE>)}.{nameof(SaveModuleDefinitionAsync)}", mod.Name, mod.ModuleGuid,
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
        protected async Task<SerializableList<DesignedModule>> GetDesignedModulesAsync() {
            using (ICacheDataProvider staticCacheDP = YetaWF.Core.IO.Caching.GetStaticCacheProvider()) {
                SerializableList<DesignedModule> list;
                GetObjectInfo<SerializableList<DesignedModule>> info = await staticCacheDP.GetAsync<SerializableList<DesignedModule>>(DESIGNEDMODULESKEY);
                if (info.Success)
                    list = info.Data;
                else { 
                    list = await DataProviderIOMode.GetDesignedModulesAsync();
                    await staticCacheDP.AddAsync(DESIGNEDMODULESKEY, list);
                }
                return list;
            }
        }
        protected async Task SaveDesignedModulesAsync(SerializableList<DesignedModule> list) {
            using (ICacheDataProvider staticCacheDP = YetaWF.Core.IO.Caching.GetStaticCacheProvider()) {
                await staticCacheDP.AddAsync<SerializableList<DesignedModule>>(DESIGNEDMODULESKEY, list);
            }
        }
        public async Task<ILockObject> LockDesignedModulesAsync() {
            return await YetaWF.Core.IO.Caching.LockProvider.LockResourceAsync(DESIGNEDMODULESKEY);
        }

        // IINSTALLABLEMODEL
        // IINSTALLABLEMODEL
        // IINSTALLABLEMODEL

        public class ModData {
            public SerializableList<TYPE> ModList { get; set; } // list of modules
            public SerializableList<SerializableFile> ImageList { get; set; } // list of image files
            public ModData() {
                ModList = new SerializableList<TYPE>();
                ImageList = new SerializableList<SerializableFile>();
            }
        }
        public async new Task<DataProviderExportChunk> ExportChunkAsync(int count, SerializableList<SerializableFile> fileList) {
            ModData data = new ModData();
            DataProviderExportChunk exp = await DataProvider.ExportChunkAsync(count, fileList);
            if (exp.ObjectList != null) {
                data.ModList = new SerializableList<TYPE>((List<TYPE>)exp.ObjectList);
                foreach (TYPE m in data.ModList) {
                    ModuleDefinition mod = (ModuleDefinition)(object)m;
                    fileList.AddRange(await Package.ProcessAllFilesAsync(mod.ModuleDataFolder));
                }
            }
            return exp;
        }
        public new async Task ImportChunkAsync(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            SerializableList<TYPE> modList = (SerializableList<TYPE>)obj;
            await DataProvider.ImportChunkAsync(chunk, fileList, modList);
        }
    }
}
