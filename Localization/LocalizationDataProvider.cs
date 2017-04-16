/* Copyright © 2017 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using YetaWF.Core.Addons;
using YetaWF.Core.IO;
using YetaWF.Core.Localize;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;
using YetaWF.Core.Support.Serializers;

namespace YetaWF.Core.Models.DataProvider {

    public class LocalizationDataProvider : IInitializeApplicationStartup {

        private static GeneralFormatter.Style LocalizationFormat = GeneralFormatter.Style.Xml;

        protected YetaWFManager Manager { get { return YetaWFManager.Manager; } }
        protected bool HaveManager { get { return YetaWFManager.HaveManager; } }

        public void InitializeApplicationStartup() {
            LocalizationSupport.Load = Load;
            LocalizationSupport.Save = Save;
            LocalizationSupport.ClearPackageData = ClearPackageData;
            LocalizationSupport.GetFiles = GetFiles;
        }

        public const string FolderName = "Localization";

        // LOAD/SAVE
        // LOAD/SAVE
        // LOAD/SAVE

        public LocalizationData Load(Package package, string type, LocalizationSupport.Location location) {

            YetaWFManager manager;
            if (location == LocalizationSupport.Location.Merge) {
                if (!LocalizationSupport.UseLocalizationResources || !YetaWFManager.HaveManager)
                    return null;// maybe too soon or async
                manager = YetaWFManager.Manager;
                if (!manager.LocalizationSupportEnabled || manager.CurrentSite == null || !manager.CurrentSite.Localization) return null;
            } else
                manager = YetaWFManager.Manager;

            string addonUrl = VersionManager.GetAddOnModuleUrl(package.Domain, package.Product);

            string file = type.Split(new char[] { '+' }).First(); // use class name, not nested class name
            file = file.Trim(new char[] { '_' }); // generated templates have classes starting or ending in _

            // check if we have this cached
            if (location == LocalizationSupport.Location.Merge && package.CachedLocalization != null) {
                Dictionary<string, LocalizationData> cachedFiles = (Dictionary<string, LocalizationData>) package.CachedLocalization;
                LocalizationData localizationData = null;
                if (cachedFiles.TryGetValue(MakeKey(file), out localizationData))
                    return localizationData;
            }

            FileData<LocalizationData> fd;
            LocalizationData data = null;

            switch (location) {
                default:
                case LocalizationSupport.Location.DefaultResources:
                    fd = new FileData<LocalizationData> {
                        BaseFolder = Path.Combine(YetaWFManager.UrlToPhysical(addonUrl), FolderName),
                        FileName = file,
                        Format = LocalizationFormat,
                        Cacheable = false,
                    };
                    data = fd.Load();
                    break;
                case LocalizationSupport.Location.InstalledResources:
                    fd = new FileData<LocalizationData> {
                        BaseFolder = Path.Combine(YetaWFManager.UrlToPhysical(addonUrl), FolderName, MultiString.ActiveLanguage),
                        FileName = file,
                        Format = LocalizationFormat,
                        Cacheable = false,
                    };
                    data = fd.Load();
                    break;
                case LocalizationSupport.Location.CustomResources: {
                    string customAddonUrl = VersionManager.GetCustomUrlFromUrl(addonUrl);
                    fd = new FileData<LocalizationData> {
                        BaseFolder = Path.Combine(YetaWFManager.UrlToPhysical(customAddonUrl), FolderName, MultiString.ActiveLanguage),
                        FileName = file,
                        Format = LocalizationFormat,
                        Cacheable = false,
                    };
                    data = fd.Load();
                    break;
                }
                case LocalizationSupport.Location.Merge: {
                    LocalizationData newData = null;
                    string customAddonUrl = VersionManager.GetCustomUrlFromUrl(addonUrl);
                    fd = new FileData<LocalizationData> {
                        BaseFolder = Path.Combine(YetaWFManager.UrlToPhysical(customAddonUrl), FolderName, MultiString.ActiveLanguage),
                        FileName = file,
                        Format = LocalizationFormat,
                        Cacheable = false,
                    };
                    newData = fd.Load();

                    if (newData != null) {
                        data = newData;
                        newData = null;
                    } else {
                        // get installed resources if available
                        fd = new FileData<LocalizationData> {
                            BaseFolder = Path.Combine(YetaWFManager.UrlToPhysical(addonUrl), FolderName, MultiString.ActiveLanguage),
                            FileName = file,
                            Format = LocalizationFormat,
                            Cacheable = false,
                        };
                        data = fd.Load();

                        if (data == null) {
                            // get default resource
                            fd = new FileData<LocalizationData> {
                                BaseFolder = Path.Combine(YetaWFManager.UrlToPhysical(addonUrl), FolderName),
                                FileName = file,
                                Format = LocalizationFormat,
                                Cacheable = false,
                            };
                            data = fd.Load();
                        }
                        if (data != null && newData != null)
                            Merge(data, newData);// merge custom data into base data
                    }
                    lock (package) {
                        if (package.CachedLocalization == null)
                            package.CachedLocalization = new Dictionary<string, LocalizationData>();
                        Dictionary<string, LocalizationData> cachedFiles = (Dictionary<string, LocalizationData>) package.CachedLocalization;
                        string key = MakeKey(file);
                        if (!cachedFiles.ContainsKey(key))
                            cachedFiles.Add(key, data);
                    }
                    break;
                }
            }
            return data;
        }

        private string MakeKey(string file) {
            if (Manager.CurrentSite == null) throw new InternalError("No current site");
            return string.Format("{0}_{1}_{2})", Manager.CurrentSite.SiteDomain, MultiString.ActiveLanguage, file);
        }

        private void Merge(LocalizationData data, LocalizationData newData) {
            foreach (LocalizationData.ClassData newCls in newData.Classes) {
                LocalizationData.ClassData cls = data.FindClass(newCls.Name);
                if (cls != null) {
                    if (!string.IsNullOrWhiteSpace(newCls.Header)) cls.Header = newCls.Header;
                    if (!string.IsNullOrWhiteSpace(newCls.Footer)) cls.Footer = newCls.Footer;
                    if (!string.IsNullOrWhiteSpace(newCls.Legend)) cls.Legend = newCls.Legend;
                    foreach (LocalizationData.PropertyData newProp in newCls.Properties) {
                        LocalizationData.PropertyData prop = data.FindProperty(newCls.Name, newProp.Name);
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
                LocalizationData.EnumData enm = data.FindEnum(newEnum.Name);
                if (enm != null) {
                    foreach (LocalizationData.EnumDataEntry newEntry in newEnum.Entries) {
                        LocalizationData.EnumDataEntry entry = enm.FindEntry(newEntry.Name);
                        if (entry != null) {
                            if (!string.IsNullOrWhiteSpace(newEntry.Caption)) entry.Caption = newEntry.Caption;
                            if (!string.IsNullOrWhiteSpace(newEntry.Description)) entry.Description = newEntry.Description;
                        }
                    }
                }
            }
            foreach (LocalizationData.StringData newString in newData.Strings) {
                LocalizationData.StringData str = data.FindStringEntry(newString.Name);
                if (str != null) {
                    if (!string.IsNullOrWhiteSpace(str.Text)) str.Text = newString.Text;
                }
            }
        }
        public void Save(Package package, string type, LocalizationSupport.Location location, LocalizationData data) {
            if (!Startup.Started || !HaveManager) throw new InternalError("Can't save resource files during startup");
            if (!Manager.LocalizationSupportEnabled) throw new InternalError("Can't save resource files during startup");
            string addonUrl = VersionManager.GetAddOnModuleUrl(package.Domain, package.Product);

            string file = type.Split(new char[] { '+' }).First(); // use class name, not nested class name
            file = file.Trim(new char[] { '_' }); // generated templates have classes starting or ending in _

            lock (package) {
                if (package.CachedLocalization != null) {
                    Dictionary<string, LocalizationData> cachedFiles = (Dictionary<string, LocalizationData>) package.CachedLocalization;
                    cachedFiles.Remove(MakeKey(file));
                }
            }

            FileData<LocalizationData> fd;
            if (data == null) {
                if (location == LocalizationSupport.Location.CustomResources) {
                    string customAddonUrl = VersionManager.GetCustomUrlFromUrl(addonUrl);
                    fd = new FileData<LocalizationData> {
                        BaseFolder = Path.Combine(YetaWFManager.UrlToPhysical(customAddonUrl), FolderName, MultiString.ActiveLanguage),
                        FileName = file,
                        Format = LocalizationFormat,
                        Cacheable = false,
                    };
                    fd.TryRemove();
                } else  if (location == LocalizationSupport.Location.InstalledResources && MultiString.ActiveLanguage != MultiString.DefaultLanguage) {
                    fd = new FileData<LocalizationData> {
                        BaseFolder = Path.Combine(YetaWFManager.UrlToPhysical(addonUrl), FolderName, MultiString.ActiveLanguage),
                        FileName = file,
                        Format = LocalizationFormat,
                        Cacheable = false,
                    };
                    fd.TryRemove();
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
                    case LocalizationSupport.Location.DefaultResources: {
                        fd = new FileData<LocalizationData> {
                            BaseFolder = Path.Combine(YetaWFManager.UrlToPhysical(addonUrl), FolderName),
                            FileName = file,
                            Format = LocalizationFormat,
                            Cacheable = false,
                        };
                        break;
                    }
                    case LocalizationSupport.Location.InstalledResources: {
                        fd = new FileData<LocalizationData> {
                            BaseFolder = Path.Combine(YetaWFManager.UrlToPhysical(addonUrl), FolderName, MultiString.ActiveLanguage),
                            FileName = file,
                            Format = LocalizationFormat,
                            Cacheable = false,
                        };
                        break;
                    }
                    case LocalizationSupport.Location.CustomResources: {
                        string customAddonUrl = VersionManager.GetCustomUrlFromUrl(addonUrl);
                        fd = new FileData<LocalizationData> {
                            BaseFolder = Path.Combine(YetaWFManager.UrlToPhysical(customAddonUrl), FolderName, MultiString.ActiveLanguage),
                            FileName = file,
                            Format = LocalizationFormat,
                            Cacheable = false,
                        };
                        break;
                    }
                    case LocalizationSupport.Location.Merge:
                        throw new InternalError("Merge can't be used when saving");
                }
                fd.TryRemove();
                fd.Add(data);
            }
            ObjectSupport.InvalidateAll();
        }
        private void ClearPackageData(Package package) {
            List<string> entries = GetFiles(package);
            foreach (var file in entries) {
                FileData<LocalizationData> fd = new FileData<LocalizationData> {
                    BaseFolder = Path.GetDirectoryName(file),
                    FileName = Path.GetFileName(file),
                    Format = LocalizationFormat,
                    Cacheable = false,
                };
                fd.Remove();
            }
        }
        public List<string> GetFiles(Package package) {
            string url = VersionManager.TryGetAddOnModuleUrl(package.Domain, package.Product);
            if (string.IsNullOrWhiteSpace(url)) return new List<string>();
            string path = Path.Combine(YetaWFManager.UrlToPhysical(url), LocalizationDataProvider.FolderName);
            FileData fdFolder = new FileData {
                BaseFolder = path,
            };
            List<string> files = fdFolder.GetNames();
            files = (from f in files select Path.Combine(path, f)).ToList();
            return files;
        }
    }
}
