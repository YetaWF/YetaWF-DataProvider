/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/AddThis#License */

using System;
using System.Collections.Generic;
using System.IO;
using YetaWF.Core.Modules;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.File {

    public class FileDataProvider {

        internal class ModuleDefinitionDataProvider<KEY, TYPE> : FileDataProvider<KEY, TYPE>, ModuleDefinitionDataProviderIOMode {
            public ModuleDefinitionDataProvider(Dictionary<string, object> options) : base(options) {
            }
            public override string GetBaseFolder() { return Path.Combine(YetaWFManager.DataFolder, ModuleDefinition.BaseFolderName, SiteIdentity.ToString()); }

            public DesignedModulesDictionary GetDesignedModules() {
                DesignedModulesDictionary modules = new DesignedModulesDictionary();
                List<Guid> modGuids = (List<Guid>)(object)FileDataProvider<KEY, TYPE>.GetListOfKeys(BaseFolder);
                foreach (var modGuid in modGuids) {
                    ModuleDefinition mod = (ModuleDefinition)(object) Get((KEY)(object)modGuid);
                    if (mod == null)
                        throw new InternalError("No ModuleDefinition for guid {0}", modGuid);
                    DesignedModule desMod = new DesignedModule() { ModuleGuid = modGuid, Name = mod.Name, Description = mod.Description, AreaName = mod.Area, };
                    modules.Add(modGuid, desMod);
                }
                return modules;
            }
        }
    }
}
