/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/AddThis#License */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Modules;

namespace YetaWF.DataProvider.SQL {

    public class SQLDataProvider {

        internal class ModuleDefinitionDataProvider<KEY, TYPE> : SQLModuleObject<KEY, TYPE>, ModuleDefinitionDataProviderIOMode {

            public ModuleDefinitionDataProvider(Dictionary<string, object> options) : base(options) { }

            public async Task<DesignedModulesDictionary> GetDesignedModulesAsync() {
                using (SQLSimpleObject<Guid, TempDesignedModule> dp = new SQLSimpleObject<Guid, TempDesignedModule>(Options)) {
                    DataProviderGetRecords<TempDesignedModule> modules = await dp.GetRecordsAsync(0, 0, null, null);
                    DesignedModulesDictionary designedMods = new DesignedModulesDictionary();
                    foreach (TempDesignedModule mod in modules.Data) {
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
        }
    }
}
