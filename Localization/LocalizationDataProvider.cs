/* Copyright © 2022 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using YetaWF.Core.IO;
using YetaWF.Core.Localize;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;
using YetaWF.Core.Support.Serializers;

namespace YetaWF.DataProvider.Localization {

    /// <summary>
    /// This class is used to install the required localization data provider support during application startup, by setting properties in the static class YetaWF.Core.IO.Localization class.
    /// This class implements the support to load and save localization files in the .\Localization and .\LocalizationCustom folders.
    /// </summary>
    /// <remarks>This class should not be instantiated and has no callable methods.
    /// It installs the localization load/save support in the framework's core YetaWF.Core.IO.Localization class during application startup.
    ///
    /// The localization data provider's load/save methods cache all localization data as needed (lazy loading).
    /// </remarks>
    public class LocalizationDataProvider : IInitializeApplicationStartup {

        private static GeneralFormatter.Style LocalizationFormat = GeneralFormatter.Style.JSON;

        internal YetaWFManager Manager { get { return YetaWFManager.Manager; } }
        internal bool HaveManager { get { return YetaWFManager.HaveManager; } }

        /// <summary>
        /// Called during application startup.
        ///
        /// Installs all required methods to load/save localization resources.
        /// </summary>
        public Task InitializeApplicationStartupAsync() {
            YetaWF.Core.IO.Localization.Load = Load;
            YetaWF.Core.IO.Localization.SaveAsync = SaveAsync;
            YetaWF.Core.IO.Localization.ClearPackageDataAsync = ClearPackageDataAsync;
            YetaWF.Core.IO.Localization.GetFilesAsync = GetFilesAsync;
            return Task.CompletedTask;
        }

        // API
        // API
        // API

        private const string LocalizationFolder = "Localization";
        private const string LocalizationCustomFolder = "LocalizationCustom";

        private string GetDefaultLanguageFolder(Package package) {
            return GetLanguageFolder(package, MultiString.DefaultLanguage);
        }
        private string GetActiveLanguageFolder(Package package) {
            return GetLanguageFolder(package, MultiString.ActiveLanguage);
        }
        private string GetCustomLanguageFolder(Package package) {
            return Path.Combine(YetaWFManager.RootFolderWebProject, LocalizationCustomFolder, MultiString.ActiveLanguage, package.LanguageDomain, package.Product);
        }
        private string GetLanguageFolder(Package package, string language) {
            return Path.Combine(YetaWFManager.RootFolderWebProject, LocalizationFolder, language, package.LanguageDomain, package.Product);
        }

        private LocalizationData? Load(Package package, string type, YetaWF.Core.IO.Localization.Location location) {

            YetaWFManager manager;
            if (location == YetaWF.Core.IO.Localization.Location.Merge) {
                if (!LocalizationSupport.UseLocalizationResources || !YetaWFManager.HaveManager)
                    return null;// maybe too soon or async
                manager = YetaWFManager.Manager;
                if (!manager.LocalizationSupportEnabled || manager.CurrentSite == null || !manager.CurrentSite.Localization) return null;
            } else
                manager = YetaWFManager.Manager;

            string defaultLanguageFolder = GetDefaultLanguageFolder(package);
            string customLanguageFolder = GetCustomLanguageFolder(package);
            string activeLanguageFolder = GetActiveLanguageFolder(package);

            string file = type.Split(new char[] { '+' }).First(); // use class name, not nested class name
            file = file.Trim(new char[] { '_' }); // generated templates have classes starting or ending in _

            // check if we have this cached
            if (location == YetaWF.Core.IO.Localization.Location.Merge && package.CachedLocalization != null) {
                Dictionary<string, LocalizationData?> cachedFiles = (Dictionary<string, LocalizationData?>) package.CachedLocalization;
                if (cachedFiles.TryGetValue(MakeKey(file), out LocalizationData? localizationData))
                    return localizationData;
            }

            FileData<LocalizationData> fd;
            LocalizationData? data = null;

            YetaWFManager.Syncify(async () => { // This must be sync because this is called from all kinds of property getters which can't be async, fortunately this is cached so it only happens once

                switch (location) {
                    default:
                    case YetaWF.Core.IO.Localization.Location.DefaultResources:
                        fd = new FileData<LocalizationData> {
                            BaseFolder = defaultLanguageFolder,
                            FileName = file,
                            Format = LocalizationFormat,
                            Cacheable = false,
                        };
                        data = await fd.LoadAsync();
                        break;
                    case YetaWF.Core.IO.Localization.Location.InstalledResources:
                        fd = new FileData<LocalizationData> {
                            BaseFolder = activeLanguageFolder,
                            FileName = file,
                            Format = LocalizationFormat,
                            Cacheable = false,
                        };
                        data = await fd.LoadAsync();
                        break;
                    case YetaWF.Core.IO.Localization.Location.CustomResources: {
                            fd = new FileData<LocalizationData> {
                                BaseFolder = customLanguageFolder,
                                FileName = file,
                                Format = LocalizationFormat,
                                Cacheable = false,
                            };
                            data = await fd.LoadAsync();
                            break;
                        }
                    case YetaWF.Core.IO.Localization.Location.Merge: {
                            LocalizationData? newData = null;
                            fd = new FileData<LocalizationData> {
                                BaseFolder = customLanguageFolder,
                                FileName = file,
                                Format = LocalizationFormat,
                                Cacheable = false,
                            };
                            newData = await fd.LoadAsync();

                            if (newData != null) {
                                data = newData;
                                newData = null;
                            } else {
                                // get installed resources if available
                                fd = new FileData<LocalizationData> {
                                    BaseFolder = activeLanguageFolder,
                                    FileName = file,
                                    Format = LocalizationFormat,
                                    Cacheable = false,
                                };
                                data = await fd.LoadAsync();

                                if (data == null) {
                                    // get default resource
                                    fd = new FileData<LocalizationData> {
                                        BaseFolder = defaultLanguageFolder,
                                        FileName = file,
                                        Format = LocalizationFormat,
                                        Cacheable = false,
                                    };
                                    data = await fd.LoadAsync();
                                }
                                if (data != null && newData != null)
                                    Merge(data, newData);// merge custom data into base data
                            }
                            lock (package) { // lock used for local data
                                if (package.CachedLocalization == null)
                                    package.CachedLocalization = new Dictionary<string, LocalizationData?>();
                                Dictionary<string, LocalizationData?> cachedFiles = (Dictionary<string, LocalizationData?>)package.CachedLocalization;
                                string key = MakeKey(file);
                                if (!cachedFiles.ContainsKey(key))
                                    cachedFiles.Add(key, data);
                            }
                            break;
                        }
                }
            });
            return data;
        }

        private string MakeKey(string file) {
            if (Manager.CurrentSite == null) throw new InternalError("No current site");
            return string.Format("{0}_{1}_{2})", Manager.CurrentSite.SiteDomain, MultiString.ActiveLanguage, file);
        }

        private void Merge(LocalizationData data, LocalizationData newData) {
            foreach (LocalizationData.ClassData newCls in newData.Classes) {
                LocalizationData.ClassData? cls = data.FindClass(newCls.Name);
                if (cls != null) {
                    if (!string.IsNullOrWhiteSpace(newCls.Header)) cls.Header = newCls.Header;
                    if (!string.IsNullOrWhiteSpace(newCls.Footer)) cls.Footer = newCls.Footer;
                    if (!string.IsNullOrWhiteSpace(newCls.Legend)) cls.Legend = newCls.Legend;
                    foreach (LocalizationData.PropertyData newProp in newCls.Properties) {
                        LocalizationData.PropertyData? prop = data.FindProperty(newCls.Name, newProp.Name);
                        if (prop != null) {
                            if (!string.IsNullOrWhiteSpace(newProp.Caption)) prop.Caption = newProp.Caption;
                            if (!string.IsNullOrWhiteSpace(newProp.Description)) prop.Description = newProp.Description;
                            if (!string.IsNullOrWhiteSpace(newProp.HelpLink)) prop.HelpLink = newProp.HelpLink;
                            if (!string.IsNullOrWhiteSpace(newProp.TextAbove)) prop.TextAbove = newProp.TextAbove;
                            if (!string.IsNullOrWhiteSpace(newProp.TextBelow)) prop.TextBelow = newProp.TextBelow;
                        }
                    }
                }
            }
            foreach (LocalizationData.EnumData newEnum in newData.Enums) {
                LocalizationData.EnumData? enm = data.FindEnum(newEnum.Name);
                if (enm != null) {
                    foreach (LocalizationData.EnumDataEntry newEntry in newEnum.Entries) {
                        LocalizationData.EnumDataEntry? entry = enm.FindEntry(newEntry.Name);
                        if (entry != null) {
                            if (!string.IsNullOrWhiteSpace(newEntry.Caption)) entry.Caption = newEntry.Caption;
                            if (!string.IsNullOrWhiteSpace(newEntry.Description)) entry.Description = newEntry.Description;
                        }
                    }
                }
            }
            foreach (LocalizationData.StringData newString in newData.Strings) {
                LocalizationData.StringData? str = data.FindStringEntry(newString.Name);
                if (str != null) {
                    if (!string.IsNullOrWhiteSpace(str.Text)) str.Text = newString.Text;
                }
            }
        }
        private async Task SaveAsync(Package package, string type, YetaWF.Core.IO.Localization.Location location, LocalizationData? data) {
            if (!Startup.Started || !HaveManager) throw new InternalError("Can't save resource files during startup");
            if (!Manager.LocalizationSupportEnabled) throw new InternalError("Can't save resource files during startup");

            string file = type.Split(new char[] { '+' }).First(); // use class name, not nested class name
            file = file.Trim(new char[] { '_' }); // generated templates have classes starting or ending in _

            lock (package) { // lock used for local data
                if (package.CachedLocalization != null) {
                    Dictionary<string, LocalizationData?> cachedFiles = (Dictionary<string, LocalizationData?>) package.CachedLocalization;
                    cachedFiles.Remove(MakeKey(file));
                }
            }

            string defaultLanguageFolder = GetDefaultLanguageFolder(package);
            string customLanguageFolder = GetCustomLanguageFolder(package);
            string activeLanguageFolder = GetActiveLanguageFolder(package);

            FileData<LocalizationData> fd;
            if (data == null) {
                if (location == YetaWF.Core.IO.Localization.Location.CustomResources) {
                    fd = new FileData<LocalizationData> {
                        BaseFolder = customLanguageFolder,
                        FileName = file,
                        Format = LocalizationFormat,
                        Cacheable = false,
                    };
                    await fd.TryRemoveAsync();
                } else if (location == YetaWF.Core.IO.Localization.Location.InstalledResources && MultiString.ActiveLanguage != MultiString.DefaultLanguage) {
                    fd = new FileData<LocalizationData> {
                        BaseFolder = activeLanguageFolder,
                        FileName = file,
                        Format = LocalizationFormat,
                        Cacheable = false,
                    };
                    await fd.TryRemoveAsync();
                } else
                    throw new InternalError("Only custom localization and non US-English installed resources can be removed");
            } else {
                // order all info by name
                if (data.Classes == null) data.Classes = new SerializableList<LocalizationData.ClassData>();
                data.Classes = new SerializableList<LocalizationData.ClassData>((from c in data.Classes orderby c.Name select c).ToList());
                foreach (LocalizationData.ClassData classData in data.Classes) {
                    if (classData.Properties == null) classData.Properties = new SerializableList<LocalizationData.PropertyData>();
                    classData.Properties = new SerializableList<LocalizationData.PropertyData>((from c in classData.Properties orderby c.Name select c).ToList());
                }
                if (data.Enums == null) data.Enums = new SerializableList<LocalizationData.EnumData>();
                data.Enums = new SerializableList<LocalizationData.EnumData>((from c in data.Enums orderby c.Name select c).ToList());
                if (data.Strings == null) data.Strings = new SerializableList<LocalizationData.StringData>();
                data.Strings = new SerializableList<LocalizationData.StringData>((from c in data.Strings orderby c.Name select c).ToList());

                switch (location) {
                    default:
                    case YetaWF.Core.IO.Localization.Location.DefaultResources: {
                        fd = new FileData<LocalizationData> {
                            BaseFolder = defaultLanguageFolder,
                            FileName = file,
                            Format = LocalizationFormat,
                            Cacheable = false,
                        };
                        break;
                    }
                    case YetaWF.Core.IO.Localization.Location.InstalledResources: {
                        fd = new FileData<LocalizationData> {
                            BaseFolder = activeLanguageFolder,
                            FileName = file,
                            Format = LocalizationFormat,
                            Cacheable = false,
                        };
                        break;
                    }
                    case YetaWF.Core.IO.Localization.Location.CustomResources: {
                        fd = new FileData<LocalizationData> {
                            BaseFolder = customLanguageFolder,
                            FileName = file,
                            Format = LocalizationFormat,
                            Cacheable = false,
                        };
                        break;
                    }
                    case YetaWF.Core.IO.Localization.Location.Merge:
                        throw new InternalError("Merge can't be used when saving");
                }
                await fd.TryRemoveAsync();
                await fd.AddAsync(data);
            }
            ObjectSupport.InvalidateAll();
        }
        private async Task ClearPackageDataAsync(Package package, string language) {
            List<string> entries = await GetFilesAsync(package, language, false);
            foreach (var file in entries) {
                FileData<LocalizationData> fd = new FileData<LocalizationData> {
                    BaseFolder = Path.GetDirectoryName(file)!,
                    FileName = Path.GetFileName(file),
                    Format = LocalizationFormat,
                    Cacheable = false,
                };
                await fd.RemoveAsync();
            }
        }
        private async Task<List<string>> GetFilesAsync(Package package, string language, bool rawName) {
            string path = GetLanguageFolder(package, language);
            List<string> files = new List<string>();
            if (await FileSystem.FileSystemProvider.DirectoryExistsAsync(path)) {
                files = await FileSystem.FileSystemProvider.GetFilesAsync(path);
                if (!rawName)
                    files = (from f in files select Path.GetFileNameWithoutExtension(f)).ToList();
                files = (from f in files select Path.Combine(path, f)).ToList();
            }
            return files;
        }
    }
}
