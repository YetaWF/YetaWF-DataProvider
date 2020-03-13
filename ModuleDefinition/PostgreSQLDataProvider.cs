/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Modules;
using YetaWF.Core.Serializers;

namespace YetaWF.DataProvider.PostgreSQL {

    internal class PostgreSQLDataProvider {

        internal class ModuleDefinitionDataProvider<KEY, TYPE> : SQLModuleObject<KEY, TYPE>, ModuleDefinitionDataProviderIOMode {

            public ModuleDefinitionDataProvider(Dictionary<string, object> options) : base(options) { }

            public async Task<SerializableList<DesignedModule>> GetDesignedModulesAsync() {
                using (SQLSimpleObject<Guid, TempDesignedModule> dp = new SQLSimpleObject<Guid, TempDesignedModule>(Options)) {
                    DataProviderGetRecords<TempDesignedModule> modules = await dp.GetRecordsAsync(0, 0, null, null);
                    SerializableList<DesignedModule> list = new SerializableList<DesignedModule>();
                    foreach (TempDesignedModule mod in modules.Data) {
                        list.Add(new DesignedModule {
                            ModuleGuid = mod.ModuleGuid,
                            Name = mod.Name,
                            Description = mod.Description,
                            AreaName = mod.DerivedAssemblyName.Replace(".", "_"),
                        });
                    }
                    return list;
                }
            }
        }
    }
}
