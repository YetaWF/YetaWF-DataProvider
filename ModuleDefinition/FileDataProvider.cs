/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using YetaWF.Core.Modules;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.File {

    public class FileDataProvider {

        internal class ModuleDefinitionDataProvider<KEY, TYPE> : FileDataProvider<KEY, TYPE>, ModuleDefinitionDataProviderIOMode {

            public ModuleDefinitionDataProvider(Dictionary<string, object> options) : base(options) { }
            public override string GetBaseFolder() { return Path.Combine(YetaWFManager.DataFolder, ModuleDefinition.BaseFolderName, SiteIdentity.ToString()); }

            public async Task<SerializableList<DesignedModule>> GetDesignedModulesAsync() {
                using (GenericModuleDefinitionDataProvider dp = new GenericModuleDefinitionDataProvider(Options)) {
                    return await dp.GetDesignedModulesAsync();
                }
            }
        }
        internal class GenericModuleDefinitionDataProvider : FileDataProvider<Guid, ModuleDefinition> {

            public GenericModuleDefinitionDataProvider(Dictionary<string, object> options) : base(options) { }
            public override string GetBaseFolder() { return Path.Combine(YetaWFManager.DataFolder, ModuleDefinition.BaseFolderName, SiteIdentity.ToString()); }

            public async Task<SerializableList<DesignedModule>> GetDesignedModulesAsync() {
                SerializableList<DesignedModule> list = new SerializableList<DesignedModule>();
                List<Guid> modGuids = await GetListOfKeysAsync(BaseFolder);
                foreach (var modGuid in modGuids) {
                    ModuleDefinition mod = await GetAsync(modGuid);
                    if (mod == null)
                        throw new InternalError("No ModuleDefinition for guid {0}", modGuid);
                    DesignedModule desMod = new DesignedModule() { ModuleGuid = modGuid, Name = mod.Name, Description = mod.Description, AreaName = mod.Area, };
                    list.Add(desMod);
                }
                return list;
            }
        }
    }
}
