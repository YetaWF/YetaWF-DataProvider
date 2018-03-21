/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
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

        public Task InitializeApplicationStartupAsync(bool firstNode) {
            ModuleDefinition.LoadModuleDefinitionAsync = LoadModuleDefinitionAsync;
            ModuleDefinition.SaveModuleDefinitionAsync = SaveModuleDefinitionAsync;
            ModuleDefinition.RemoveModuleDefinitionAsync = RemoveModuleDefinitionAsync;
            DesignedModules.LoadDesignedModulesAsync = LoadDesignedModulesAsync;
            ModuleDefinition.GetModulesAsync = GetModulesAsync;
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
            GetObjectInfo<ModuleDefinition> objInfo = await Caching.SharedCacheProvider.GetAsync<ModuleDefinition>(CacheKey(guid));
            if (!objInfo.Success)
                return modInfo;
            modInfo.Success = true;
            modInfo.Module = objInfo.Data;
            return modInfo;
        }
        private Task SetCachedModuleAsync(ModuleDefinition mod) {
            return Caching.SharedCacheProvider.AddAsync(CacheKey(mod.ModuleGuid), mod);
        }
        private Task SetEmptyCachedModuleAsync(Guid guid) {
            return Caching.SharedCacheProvider.AddAsync<ModuleDefinition>(CacheKey(guid), null);
        }
        private Task RemoveCachedModuleAsync(Guid guid) {
            return Caching.SharedCacheProvider.RemoveAsync(CacheKey(guid));
        }

        // Implementation

        private async Task<List<DesignedModule>> LoadDesignedModulesAsync() {
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                return await modDP.LoadDesignedModulesAsync();
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
            GetCachedModuleInfo modInfo = await GetCachedModuleAsync(guid);
            if (modInfo.Success && modInfo.Module != null)
                return modInfo.Module;
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                ModuleDefinition mod = await modDP.LoadModuleDefinitionAsync(guid);
                if (mod != null)
                    await SetCachedModuleAsync(mod);
                else
                    await SetEmptyCachedModuleAsync(guid);
                return mod;
            }
        }
        private async Task SaveModuleDefinitionAsync(ModuleDefinition mod, IModuleDefinitionIO dataProvider) {
            using (dataProvider) {
                await dataProvider.SaveModuleDefinitionAsync(mod);
            }
            await SetCachedModuleAsync(mod);
        }
        private async Task<bool> RemoveModuleDefinitionAsync(Guid guid) {
            await RemoveCachedModuleAsync(guid);
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                return await modDP.RemoveModuleDefinitionAsync(guid);
            }
        }
    }

    public interface ModuleDefinitionDataProviderIOMode {
        Task<DesignedModulesDictionary> GetDesignedModulesAsync();
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
    public class GenericModuleDefinitionDataProvider : ModuleDefinitionDataProvider<Guid, ModuleDefinition> { }

    // Loads/saves a specific module type
    public class ModuleDefinitionDataProvider<KEY, TYPE> : DataProviderImpl, IModuleDefinitionIO {

        private static AsyncLock _lockObject = new AsyncLock();

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

        public async Task<DataProviderGetRecords<TYPE>> GetModulesAsync(int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters) {
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
            mod.DateUpdated = DateTime.UtcNow;
            SaveImages(mod.ModuleGuid, mod);
            await mod.ModuleSavingAsync();
            using (DataProvider.StartTransaction()) {
                UpdateStatusEnum status = await DataProvider.UpdateAsync((KEY)(object)mod.ModuleGuid, (KEY)(object)mod.ModuleGuid, (TYPE)(object)mod);
                if (status != UpdateStatusEnum.OK)
                    if (!await DataProvider.AddAsync((TYPE)(object)mod))
                        throw new InternalError("Can't add module definition for {0}", mod.ModuleGuid);
                await DataProvider.CommitTransactionAsync();
                DesignedModulesDictionary modules;
                if (!PermanentManager.TryGetObject<DesignedModulesDictionary>(out modules) || modules == null)
                    return; // don't have a list, no need to build it (yet)
                if (modules.ContainsKey(mod.ModuleGuid)) {
                    DesignedModule desMod = modules[mod.ModuleGuid];
                    desMod.Name = mod.Name;
                } else {
                    DesignedModule desMod = new DesignedModule() {
                        ModuleGuid = mod.ModuleGuid,
                        Description = mod.Description,
                        Name = mod.Name,
                        AreaName = mod.Area,
                    };
                    modules.Add(mod.ModuleGuid, desMod);
                }
            }
        }
        public async Task<bool> RemoveModuleDefinitionAsync(Guid key) {
            bool status = false;
            await StringLocks.DoActionAsync(key.ToString(), async () => {
                try {
                    ModuleDefinition mod = await LoadModuleDefinitionAsync(key);
                    if (mod != null)
                        await mod.ModuleRemovingAsync();
                } catch (Exception) { }
                DesignedModulesDictionary dict = GetDesignedModules();
                DesignedModule desMod;
                if (!dict.TryGetValue(key, out desMod))
                    status = false;
                else {
                    dict.Remove(key);
                    status = await DataProvider.RemoveAsync((KEY)(object)key);
                }

                if (status) {
                    // remove the data folder (if any)
                    string dir = ModuleDefinition.GetModuleDataFolder(key);
                    DirectoryIO.DeleteFolder(dir);
                }
            });
            return status;
        }

        // DESIGNED MODULES
        // DESIGNED MODULES
        // DESIGNED MODULES

        // Designed modules are site specific and DesignedModules is a permanent site-specific object

        public async Task<List<DesignedModule>> LoadDesignedModulesAsync() {
            List<DesignedModule> list = new List<DesignedModule>();
            if (await DataProvider.IsInstalledAsync()) {// a new site may not have the data installed yet
                DesignedModulesDictionary dict = GetDesignedModules();
                list = (from d in dict select d.Value).ToList();
            }
            return list;
        }
        // This is cached so we keep it sync for simplicity
        protected DesignedModulesDictionary GetDesignedModules() {
            DesignedModulesDictionary modules;

            if (PermanentManager.TryGetObject<DesignedModulesDictionary>(out modules))
                return modules;

            using (_lockObject.Lock()) { // lock this so we only do this once
                // See if we already have it as a permanent object
                if (PermanentManager.TryGetObject<DesignedModulesDictionary>(out modules))
                    return modules;

                // Load the designed pages
                YetaWFManager.Syncify(async () => { // only done once at startup so sync it to keep things simple
                    modules = await DataProviderIOMode.GetDesignedModulesAsync();
                });
                PermanentManager.AddObject<DesignedModulesDictionary>(modules);
            }
            return modules;
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
        public new async Task<bool> ExportChunkAsync(int count, SerializableList<SerializableFile> fileList) {
            ModData data = new ModData();
            DataProviderExportChunk exp = await DataProvider.ExportChunkAsync(count, fileList);
            if (exp.ObjectList != null) {
                data.ModList = new SerializableList<TYPE>((List<TYPE>)exp.ObjectList);
                foreach (TYPE m in data.ModList) {
                    ModuleDefinition mod = (ModuleDefinition)(object)m;
                    fileList.AddRange(Package.ProcessAllFiles(mod.ModuleDataFolder));
                }
            }
            return exp.More;
        }
        public new async Task ImportChunkAsync(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            ModData data = (ModData)obj;
            await DataProvider.ImportChunkAsync(chunk, fileList, data.ModList);
        }
    }
}
