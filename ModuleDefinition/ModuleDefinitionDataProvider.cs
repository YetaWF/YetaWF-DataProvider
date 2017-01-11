/* Copyright © 2017 Softel vdm, Inc. - http://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.IO;
using YetaWF.Core.Models;
using YetaWF.Core.Modules;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;
using YetaWF.Core.Support.Serializers;

namespace YetaWF.DataProvider
{
    public class GenericModuleDefinitionDataProviderImpl : IInitializeApplicationStartup {

        // STARTUP
        // STARTUP
        // STARTUP

        public void InitializeApplicationStartup() {
            ModuleDefinition.LoadModuleDefinition = LoadModuleDefinition;
            ModuleDefinition.SaveModuleDefinition = SaveModuleDefinition;
            ModuleDefinition.RemoveModuleDefinition = RemoveModuleDefinition;
            DesignedModules.LoadDesignedModules = LoadDesignedModules;
            ModuleDefinition.GetModules = GetModules;
        }

        // CACHE
        // CACHE
        // CACHE

        private string CacheKey(Guid guid) {
            return string.Format("__Mod_{0}_{1}", YetaWFManager.Manager.CurrentSite.Identity, guid);
        }
        private static object EmptyCachedObject = new object();
        private bool GetModule(Guid guid, out ModuleDefinition mod) {
            mod = null;
            object o = YetaWFManager.Manager.CurrentContext.Cache[CacheKey(guid)];
            if (o == null)
                return false;
            if (o == EmptyCachedObject)
                return true;
            mod = (ModuleDefinition)new GeneralFormatter().Deserialize((byte[])o);
            return true;
        }
        private void SetModule(ModuleDefinition mod) {
            YetaWFManager.Manager.CurrentContext.Cache[CacheKey(mod.ModuleGuid)] = new GeneralFormatter().Serialize(mod);
        }
        private void SetEmptyModule(Guid guid) {
            YetaWFManager.Manager.CurrentContext.Cache[CacheKey(guid)] = EmptyCachedObject;
        }
        private void RemoveModule(Guid guid) {
            YetaWFManager.Manager.CurrentContext.Cache.Remove(CacheKey(guid));
        }

        // Implementation

        private List<DesignedModule> LoadDesignedModules() {
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                return modDP.LoadDesignedModules();
            }
        }
        private void GetModules(ModuleDefinition.ModuleBrowseInfo info) {
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                int total;
                info.Modules = modDP.GetModules(info.Skip, info.Take, info.Sort, info.Filters, out total);
                info.Total = total;
            }
        }
        private ModuleDefinition LoadModuleDefinition(Guid guid) {
            ModuleDefinition mod;
            if (GetModule(guid, out mod))
                return mod;
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                mod = modDP.LoadModuleDefinition(guid);
                if (mod != null)
                    SetModule(mod);
                else
                    SetEmptyModule(guid);
                return mod;
            }
        }
        private void SaveModuleDefinition(ModuleDefinition mod, IModuleDefinitionIO dataProvider) {
            SetModule(mod);
            using (dataProvider) {
                dataProvider.SaveModuleDefinition(mod);
            }
        }
        private bool RemoveModuleDefinition(Guid guid) {
            RemoveModule(guid);
            using (GenericModuleDefinitionDataProvider modDP = new GenericModuleDefinitionDataProvider()) {
                return modDP.RemoveModuleDefinition(guid);
            }
        }
    }

    // Loads/saves any module and creates the appropriate module type
    public class GenericModuleDefinitionDataProvider : ModuleDefinitionDataProvider<Guid, ModuleDefinition> { }

    // Loads/saves a specific module type
    public class ModuleDefinitionDataProvider<KEY, TYPE> : DataProviderImpl, IModuleDefinitionIO {

        private static object _lockObject = new object();

        // IMPLEMENTATION
        // IMPLEMENTATION
        // IMPLEMENTATION

        public ModuleDefinitionDataProvider() : base(YetaWFManager.Manager.CurrentSite.Identity) { SetDataProvider(DataProvider); }
        public ModuleDefinitionDataProvider(int siteIdentity) : base(siteIdentity) { SetDataProvider(DataProvider); }

        // SQL, File
        private IDataProvider<KEY, TYPE> DataProvider {
            get {
                if (_dataProvider == null) {
                    WebConfigHelper.IOModeEnum ioMode = GetIOMode(ModuleDefinition.BaseFolderName);

                    if (typeof(TYPE).Name != "ModuleDefinition") {
                        Package package = Package.GetPackageFromAssembly(typeof(TYPE).Assembly);
                        AreaName = ModuleDefinition.BaseFolderName + "_" + package.AreaName + "_" + typeof(TYPE).Name;
                    }

                    switch (ioMode) {
                        default:
                        case WebConfigHelper.IOModeEnum.File:
                            _dataProvider = new YetaWF.DataProvider.FileDataProvider<KEY, TYPE>(
                                Path.Combine(YetaWFManager.DataFolder, ModuleDefinition.BaseFolderName, SiteIdentity.ToString()),
                                CurrentSiteIdentity: SiteIdentity,
                                Cacheable: true);
                            break;
                        case WebConfigHelper.IOModeEnum.Sql:
                            _dataProvider = new YetaWF.DataProvider.SQLDerivedObjectDataProvider<KEY, TYPE, ModuleDefinition>(AreaName, SQLDbo, SQLConn,
                                ModuleDefinition.BaseFolderName,
                                CurrentSiteIdentity: SiteIdentity,
                                Cacheable: true);
                            break;
                    }
                }
                return _dataProvider;
            }
        }
        private IDataProvider<KEY, TYPE> _dataProvider { get; set; }

        public List<TYPE> GetModules(int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, out int total) {
            return DataProvider.GetRecords(skip, take, sort, filters, out total);
        }

        // LOAD/SAVE
        // LOAD/SAVE
        // LOAD/SAVE

        /// <summary>
        /// Load the module definition
        /// </summary>
        /// <returns>ModuleDefinition or null if module doesn't exist</returns>
        public ModuleDefinition LoadModuleDefinition(Guid key) {
            return (ModuleDefinition) (object) DataProvider.Get((KEY)(object) key);
        }

        /// <summary>
        /// Save the module definition
        /// </summary>
        public void SaveModuleDefinition(ModuleDefinition mod) {
            mod.DateUpdated = DateTime.UtcNow;
            SaveImages(mod.ModuleGuid, mod);
            mod.ModuleSaving();
            lock (_lockObject) {
                UpdateStatusEnum status = DataProvider.Update((KEY) (object) mod.ModuleGuid, (KEY) (object) mod.ModuleGuid, (TYPE) (object) mod);
                if (status != UpdateStatusEnum.OK)
                    if (!DataProvider.Add((TYPE) (object) mod))
                        throw new InternalError("Can't add module definition for {0}", mod.ModuleGuid);
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
                        AreaName = mod.AreaName,
                    };
                    modules.Add(mod.ModuleGuid, desMod);
                }
            }
        }
        public bool RemoveModuleDefinition(Guid key) {
            bool status = false;
            StringLocks.DoAction(key.ToString(), () => {

                try {
                    ModuleDefinition mod = LoadModuleDefinition(key);
                    if (mod != null)
                        mod.ModuleRemoving();
                } catch (Exception) { }
                DesignedModulesDictionary dict = GetDesignedModules();
                DesignedModule desMod;
                if (!dict.TryGetValue(key, out desMod))
                    status = false;
                else {
                    dict.Remove(key);
                    status = DataProvider.Remove((KEY) (object) key);
                }

                if (status) {
                    // remove the data folder (if any)
                    string dir = ModuleDefinition.GetModuleDataFolder(key);
                    try {
#if DEBUG // avoid exception spam
                        if (Directory.Exists(dir))
#endif
                            Directory.Delete(dir, true);
                    } catch (Exception) { }
                }
            });
            return status;
        }

        // DESIGNED MODULES
        // DESIGNED MODULES
        // DESIGNED MODULES

        // Designed modules are site specific and DesignedModules is a permanent site-specific object

        public List<DesignedModule> LoadDesignedModules() {
            List<DesignedModule> list = new List<DesignedModule>();
            if (DataProvider.IsInstalled()) {// a new site may not have the data installed yet
                DesignedModulesDictionary dict = GetDesignedModules();
                list = (from d in dict select d.Value).ToList();
            }
            return list;
        }
        protected DesignedModulesDictionary GetDesignedModules() {
            DesignedModulesDictionary modules;

            if (PermanentManager.TryGetObject<DesignedModulesDictionary>(out modules))
                return modules;

            lock (_lockObject) { // lock this so we only do this once
                // See if we already have it as a permanent object
                if (PermanentManager.TryGetObject<DesignedModulesDictionary>(out modules))
                    return modules;

                // Load the designed pages
                switch (GetIOMode(ModuleDefinition.BaseFolderName)) {
                    default:
                        throw new InternalError("IOMode undetermined - this means we don't have a valid data provider");
                    case WebConfigHelper.IOModeEnum.File:
                        modules = GetDesignedModules_File();
                        break;
                    case WebConfigHelper.IOModeEnum.Sql:
                        modules = GetDesignedModules_Sql();
                        break;
                }
                PermanentManager.AddObject<DesignedModulesDictionary>(modules);
            }
            return modules;
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
        private DesignedModulesDictionary GetDesignedModules_Sql() {
            using (SQLSimpleObjectDataProvider<Guid, TempDesignedModule> dp = new SQLSimpleObjectDataProvider<Guid, TempDesignedModule>(ModuleDefinition.BaseFolderName, SQLDbo, SQLConn, CurrentSiteIdentity: SiteIdentity)) {
                IDataProvider<Guid, TempDesignedModule> dataProvider = dp;
                int total;
                List<TempDesignedModule> modules = dataProvider.GetRecords(0, 0, null, null, out total);
                DesignedModulesDictionary designedMods = new DesignedModulesDictionary();
                foreach (TempDesignedModule mod in modules) {
                    designedMods.Add(mod.ModuleGuid, new DesignedModule {
                        ModuleGuid = mod.ModuleGuid,
                        Name = mod.Name,
                        Description = mod.Description,
                        AreaName = mod.DerivedAssemblyName.Replace(".", "_"),
                    });
                }
                return designedMods;
            }
        }

        private DesignedModulesDictionary GetDesignedModules_File() {
            DesignedModulesDictionary modules = new DesignedModulesDictionary();
            List<Guid> modGuids = (List<Guid>)(object) DataProvider.GetKeyList();
            foreach (var modGuid in modGuids) {
                ModuleDefinition mod = (ModuleDefinition) (object) DataProvider.Get((KEY)(object)modGuid);
                if (mod == null)
                    throw new InternalError("No ModuleDefinition for guid {0}", modGuid);
                DesignedModule desMod = new DesignedModule() { ModuleGuid = modGuid, Name = mod.Name, Description = mod.Description, AreaName = mod.AreaName, };
                modules.Add(modGuid, desMod);
            }
            return modules;
        }

        // IINSTALLABLEMODEL
        // IINSTALLABLEMODEL
        // IINSTALLABLEMODEL

        public bool IsInstalled() {
            return DataProvider.IsInstalled();
        }
        public bool InstallModel(List<string> errorList) {
            return DataProvider.InstallModel(errorList);
        }
        public bool UninstallModel(List<string> errorList) {
            return DataProvider.UninstallModel(errorList);
        }
        public class ModData {
            public SerializableList<TYPE> ModList { get; set; } // list of modules
            public SerializableList<SerializableFile> ImageList { get; set; } // list of image files
            public ModData() {
                ModList = new SerializableList<TYPE>();
                ImageList = new SerializableList<SerializableFile>();
            }
        }
        public void AddSiteData() {
            DataProvider.AddSiteData();
        }
        public void RemoveSiteData() {
            DataProvider.RemoveSiteData();
        }
        public bool ExportChunk(int count, SerializableList<SerializableFile> fileList, out object obj) {

            ModData data = new ModData();
            obj = data;

            object mods;
            bool status = DataProvider.ExportChunk(count, fileList, out mods);
            if (mods != null) {
                data.ModList = (SerializableList<TYPE>)mods;
                foreach (TYPE m in data.ModList) {
                    ModuleDefinition mod = (ModuleDefinition)(object)m;
                    fileList.AddRange(Package.ProcessAllFiles(mod.ModuleDataFolder));
                }
            }
            return status;
        }
        public void ImportChunk(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            ModData data = (ModData) obj;
            DataProvider.ImportChunk(chunk, fileList, data.ModList);
        }
    }
}
