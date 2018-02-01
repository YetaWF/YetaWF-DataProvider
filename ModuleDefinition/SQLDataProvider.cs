/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/AddThis#License */

using System;
using System.Collections.Generic;
using YetaWF.Core.Modules;
using YetaWF.DataProvider.SQL2;

namespace YetaWF.DataProvider.SQL {

    public class SQLDataProvider {

        internal class ModuleDefinitionDataProvider<KEY, TYPE> : SQLModuleObject<KEY, TYPE>, ModuleDefinitionDataProviderIOMode {
            public ModuleDefinitionDataProvider(Dictionary<string, object> options) : base(options) { }

            public DesignedModulesDictionary GetDesignedModules() {
                using (SQLSimpleObject<Guid, TempDesignedModule> dp = new SQLSimpleObject<Guid, TempDesignedModule>(Options)) {
                    int total;
                    List<TempDesignedModule> modules = dp.GetRecords(0, 0, null, null, out total);
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
        }
    }
}
