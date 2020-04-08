/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider;
using YetaWF.Core.Modules;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.PostgreSQL {

    internal class PostgreSQLDataProvider {

        internal class ModuleDefinitionDataProvider<KEY, TYPE> : SQLModuleObject<KEY, TYPE>, ModuleDefinitionDataProviderIOMode {

            public ModuleDefinitionDataProvider(Dictionary<string, object> options) : base(options) { }

            public async Task<SerializableList<DesignedModule>> GetDesignedModulesAsync() {
                using (SQLModuleObject<Guid, TempDesignedModule> dp = new SQLModuleObject<Guid, TempDesignedModule>(Options)) {
                    DataProviderGetRecords<TempDesignedModule> modules = await dp.GetRecordsAsync(0, 0, null, null);
                    SerializableList<DesignedModule> list = new SerializableList<DesignedModule>();
                    foreach (TempDesignedModule mod in modules.Data) {
                        ModuleDefinition modInstance = null;
                        try {
                            Type tp = null;
                            Assembly asm = Assemblies.Load(mod.DerivedAssemblyName);
                            tp = asm.GetType(mod.DerivedDataType);
                            modInstance = (ModuleDefinition)Activator.CreateInstance(tp);
                        } catch (Exception) { }
                        list.Add(new DesignedModule {
                            ModuleGuid = mod.ModuleGuid,
                            Name = mod.Name,
                            Description = modInstance?.Description,
                            AreaName = mod.DerivedAssemblyName.Replace(".", "_"),
                        });
                    }
                    return list;
                }
            }
        }
    }
}
