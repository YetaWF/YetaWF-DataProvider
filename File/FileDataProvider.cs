/* Copyright © 2018 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using YetaWF.Core;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.IO;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Serializers;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider {

    public class FileIdentityCount {

        public const int IDENTITY_SEED = 1000;

        public FileIdentityCount() { Count = IDENTITY_SEED; }
        public FileIdentityCount(int seed) { Count = seed; }
        public int Count { get; set; }
    }

    public class FileDataProviderBase {
        public static readonly string ExternalName = "File";
    }

    public class FileDataProvider<KEYTYPE, OBJTYPE> : FileDataProviderBase, IDataProvider<KEYTYPE, OBJTYPE>, IDataProviderTransactions {

        public Dictionary<string, object> Options { get; private set; }
        public Package Package { get; private set; }
        public string Dataset { get; protected set; }
        public string BaseFolder { get; private set; }
        public int SiteIdentity { get; private set; }
        public bool UseIdentity { get; set; }
        public int IdentitySeed { get; private set; }
        public bool Cacheable { get; private set; }
        public bool Logging { get; private set; }

        public Func<string, object, object> CalculatedPropertyCallback { get; set; }

        private const int ChunkSize = 100;

        public FileDataProvider(Dictionary<string, object> options) {
            Options = options;
            if (!Options.ContainsKey("Package") || !(Options["Package"] is Package))
                throw new InternalError($"No Package for data provider {GetType().FullName}");
            Package = (Package)Options["Package"];
            if (!Options.ContainsKey("Dataset") || !(Options["Dataset"] is string))
                throw new InternalError($"No Dataset for data provider {GetType().FullName}");
            Dataset = (string)Options["Dataset"];
            if (Options.ContainsKey("SiteIdentity") && Options["SiteIdentity"] is int)
                SiteIdentity = Convert.ToInt32(Options["SiteIdentity"]);
            if (Options.ContainsKey("IdentitySeed") && Options["IdentitySeed"] is int)
                IdentitySeed = Convert.ToInt32(Options["IdentitySeed"]);
            if (Options.ContainsKey("Cacheable") && Options["Cacheable"] is bool)
                Cacheable = Convert.ToBoolean(Options["Cacheable"]);
            if (Options.ContainsKey("Logging") && Options["Logging"] is bool)
                Logging = Convert.ToBoolean(Options["Logging"]);

            UseIdentity = !string.IsNullOrWhiteSpace(IdentityName);

            this.IdentitySeed = IdentitySeed == 0 ? FileIdentityCount.IDENTITY_SEED : IdentitySeed;

            BaseFolder = GetBaseFolder();

            DisposableTracker.AddObject(this);
        }

        public virtual string GetBaseFolder() { throw new InternalError($"Override GetBaseFolder() in {GetType().FullName}"); }

        public void Dispose() { Dispose(true); }
        protected virtual void Dispose(bool disposing) { if (disposing) DisposableTracker.RemoveObject(this); }

        public string Key1Name { get { return GetKey1Name(); } }
        public string IdentityName { get { return GetIdentityName(); } }

        public DataProviderTransaction StartTransaction() {
            throw new NotSupportedException($"{nameof(StartTransaction)} is not supported");
        }
        public void CommitTransaction() {
            throw new NotSupportedException($"{nameof(CommitTransaction)} is not supported");
        }
        public void AbortTransaction() {
            throw new NotSupportedException($"{nameof(AbortTransaction)} is not supported");
        }

        private string GetKey1Name() {
            if (_key1Name == null) {
                // find primary key
                foreach (var prop in ObjectSupport.GetPropertyData(typeof(OBJTYPE))) {
                    if (prop.HasAttribute(Data_PrimaryKey.AttributeName)) {
                        _key1Name = prop.Name;
                        return prop.Name;
                    }
                }
                throw new InternalError("Primary key not defined in {0}", typeof(OBJTYPE).FullName);
            }
            return _key1Name;
        }
        private string _key1Name;

        protected string GetIdentityName() {
            if (_identityName == null) {
                // find identity
                foreach (var prop in ObjectSupport.GetPropertyData(typeof(OBJTYPE))) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        _identityName = prop.Name;
                        return _identityName;
                    }
                }
                _identityName = "";
            }
            return _identityName;
        }
        private string _identityName;

        private const string InternalFilePrefix = "__";

        private FileData<OBJTYPE> GetFileDataObject(KEYTYPE key) {
            //if (typeof(KEYTYPE) == typeof(string))
            //    key = (KEYTYPE) (object) FileData.MakeValidFileName((string) (object) key);

            //string fullPath = Path.Combine(BaseFolder, key.ToString());
            //string baseFolder = Path.GetDirectoryName(fullPath);
            //string fileName = Path.GetFileName(fullPath);

            FileData<OBJTYPE> fd = new FileData<OBJTYPE> {
                BaseFolder = BaseFolder,
                FileName = key.ToString(),
                Cacheable = Cacheable
            };
            return fd;
        }

        public OBJTYPE Get(KEYTYPE key, bool SpecificType = false) {
            FileData<OBJTYPE> fd = GetFileDataObject(key);
            if (SpecificType) {
                OBJTYPE o = fd.Load(SpecificType: SpecificType);
                if (o == null) return default(OBJTYPE);
                return UpdateCalculatedProperties(o);
            } else
                return UpdateCalculatedProperties(fd.Load());
        }

        public bool Add(OBJTYPE obj) {

            PropertyInfo piKey = ObjectSupport.GetProperty(typeof(OBJTYPE), Key1Name);
            KEYTYPE key = (KEYTYPE)piKey.GetValue(obj);

            if (!string.IsNullOrWhiteSpace(IdentityName)) {
                // using identity
                int identity = 0;
                PropertyInfo piIdent = ObjectSupport.GetProperty(typeof(OBJTYPE), IdentityName);
                if (piIdent == null) throw new InternalError("Type {0} has no identity property named {1}", typeof(OBJTYPE).FullName, IdentityName);
                if (piIdent.PropertyType != typeof(int)) throw new InternalError("FileDataProvider only supports object identities of type int");

                FileData<FileIdentityCount> fdIdent = new FileData<FileIdentityCount> {
                    BaseFolder = BaseFolder,
                    FileName = InternalFilePrefix + IdentityName,
                };
                StringLocks.DoAction("YetaWF##Identity_" + BaseFolder, () => {
                    FileIdentityCount ident = fdIdent.Load();
                    if (ident == null) { // new
                        ident = new FileIdentityCount(IdentitySeed);
                        fdIdent.Add(ident);
                    } else { // existing
                        ++ident.Count;
                        fdIdent.UpdateFile(fdIdent.FileName, ident);
                    }
                    identity = ident.Count;
                });
                piIdent.SetValue(obj, identity);
                if (Key1Name == IdentityName)
                    key = (KEYTYPE)(object)identity;
            }
            FileData<OBJTYPE> fd = GetFileDataObject(key);
            return fd.Add(obj);
        }
        public UpdateStatusEnum Update(KEYTYPE origKey, KEYTYPE newKey, OBJTYPE obj) {
            return UpdateFile(origKey, newKey, obj);
        }
        private UpdateStatusEnum UpdateFile(KEYTYPE origKey, KEYTYPE newKey, OBJTYPE obj) {
            FileData<OBJTYPE> fd = GetFileDataObject(origKey);
            return fd.UpdateFile(newKey.ToString(), obj);
        }
        public bool Remove(KEYTYPE key) {
            FileData<OBJTYPE> fd = GetFileDataObject(key);
            return fd.TryRemove();
        }
        public static List<KEYTYPE> GetListOfKeys(string baseFolder) {
            FileData fd = new FileData {
                BaseFolder = baseFolder,
            };
            List<string> files = fd.GetNames();
            files = (from string f in files where !f.StartsWith(InternalFilePrefix) && f != Globals.DontDeployMarker select f).ToList<string>();

            if (typeof(KEYTYPE) == typeof(string))
                return (List<KEYTYPE>)(object)files;
            else if (typeof(KEYTYPE) == typeof(Guid))
                return (from string f in files select (KEYTYPE)(object)new Guid(f)).ToList<KEYTYPE>();
            else
                throw new InternalError("FileDataProvider only supports object keys of type string or Guid");
        }

        // GETRECORDS
        // GETRECORDS
        // GETRECORDS
        public OBJTYPE GetOneRecord(List<DataProviderFilterInfo> filters, List<JoinData> Joins = null) {
            if (Joins != null) throw new InternalError("Joins not supported");
            int total;
            IDataProvider<KEYTYPE, OBJTYPE> iData = (IDataProvider<KEYTYPE, OBJTYPE>)this;
            List<OBJTYPE> objs = iData.GetRecords(0, 1, null, filters, out total);
            return UpdateCalculatedProperties(objs.FirstOrDefault());
        }
        public List<OBJTYPE> GetRecords(int skip, int take, List<DataProviderSortInfo> sort, List<DataProviderFilterInfo> filters, out int total, List<JoinData> Joins = null, bool SpecificType = false) {
            if (Joins != null) throw new InternalError("Joins not supported");
            FileData fd = new FileData {
                BaseFolder = this.BaseFolder,
            };
            List<string> files = fd.GetNames();

            IDataProvider<KEYTYPE, OBJTYPE> iData = (IDataProvider<KEYTYPE, OBJTYPE>)this;
            List<OBJTYPE> objects = new List<OBJTYPE>();

            foreach (string file in files) {

                if (file.StartsWith(InternalFilePrefix) || file == Globals.DontDeployMarker) // internal file
                    continue;

                KEYTYPE key;
                if (typeof(KEYTYPE) == typeof(string))
                    key = (KEYTYPE)(object)file;
                else if (typeof(KEYTYPE) == typeof(Guid))
                    key = (KEYTYPE)(object)new Guid(file);
                else if (typeof(KEYTYPE) == typeof(int))
                    key = (KEYTYPE)(object)Convert.ToInt32(file);
                else
                    throw new InternalError("FileDataProvider only supports object keys of type string, int or Guid");
                OBJTYPE obj = iData.Get(key, SpecificType: SpecificType);

                if (SpecificType) {
                    if (obj == null || typeof(OBJTYPE) != obj.GetType())
                        continue;
                } else {
                    if (obj == null)
                        throw new InternalError("Object in file {0} is invalid", file);
                }
                objects.Add(obj);

                if (skip == 0 && sort == null && filters == null) {
                    if (objects.Count == take)
                        break;
                }
            }
            foreach (OBJTYPE obj in objects)
                UpdateCalculatedProperties(obj);
            objects = DataProviderImpl<OBJTYPE>.Filter(objects, filters);
            total = objects.Count;
            objects = DataProviderImpl<OBJTYPE>.Sort(objects, sort);

            if (skip > 0)
                objects = objects.Skip(skip).ToList<OBJTYPE>();
            if (take > 0)
                objects = objects.Take(take).ToList<OBJTYPE>();
            return objects;
        }
        private OBJTYPE UpdateCalculatedProperties(OBJTYPE obj) {
            if (CalculatedPropertyCallback == null) return obj;
            List<PropertyData> props = ObjectSupport.GetPropertyData(typeof(OBJTYPE));
            props = (from p in props where p.CalculatedProperty select p).ToList();
            foreach (PropertyData prop in props) {
                obj = (OBJTYPE)CalculatedPropertyCallback(prop.Name, obj);
            }
            return obj;
        }

        // REMOVE RECORDS
        // REMOVE RECORDS
        // REMOVE RECORDS

        public int RemoveRecords(List<DataProviderFilterInfo> filters) {
            FileData fd = new FileData {
                BaseFolder = this.BaseFolder,
            };
            List<string> files = fd.GetNames();

            IDataProvider<KEYTYPE, OBJTYPE> iData = (IDataProvider<KEYTYPE, OBJTYPE>)this;

            int total = 0;
            foreach (string file in files) {
                if (file.StartsWith(InternalFilePrefix) || file == Globals.DontDeployMarker) // internal file
                    continue;
                KEYTYPE key;
                if (typeof(KEYTYPE) == typeof(string))
                    key = (KEYTYPE)(object)file;
                else if (typeof(KEYTYPE) == typeof(Guid))
                    key = (KEYTYPE)(object)new Guid(file);
                else if (typeof(KEYTYPE) == typeof(int))
                    key = (KEYTYPE)(object)Convert.ToInt32(file);
                else
                    throw new InternalError("FileDataProvider only supports object keys of type string, int or Guid");
                OBJTYPE obj = iData.Get(key);
                if (obj == null)
                    throw new InternalError("Object in file {0} is invalid", file);

                if (DataProviderImpl<OBJTYPE>.Filter(new List<OBJTYPE> { obj }, filters).Count > 0) {
                    FileData<OBJTYPE> fdtemp = GetFileDataObject(key);
                    if (fdtemp.TryRemove())
                        total++;
                }
            }
            if (filters == null)
                RemoveFolderIfEmpty(BaseFolder);
            return total;
        }

        private void RemoveFolderIfEmpty(string path) {
            // delete the folder if it's empty now
            if (Directory.GetDirectories(path).Count() == 0) {
                string[] files = Directory.GetFiles(path);
                bool empty = true;
                foreach (string file in files) {
                    if (!Path.GetFileName(file).StartsWith(InternalFilePrefix) && file != Globals.DontDeployMarker) {// internal file
                        empty = false;
                        break;
                    }
                }
                if (empty) {
                    DirectoryIO.DeleteFolder(BaseFolder);
                }
            }
        }

        // INSTALL/UNINSTALL
        // INSTALL/UNINSTALL
        // INSTALL/UNINSTALL

        public bool IsInstalled() {
            return Directory.Exists(BaseFolder);
        }

        public bool InstallModel(List<string> errorList) {
            try {
                if (!Directory.Exists(BaseFolder))
                    Directory.CreateDirectory(BaseFolder);
                return true;
            } catch (Exception exc) {
                errorList.Add(string.Format("{0}: {1}", BaseFolder, exc.Message));
                return false;
            }
        }
        public bool UninstallModel(List<string> errorList) {
            try {
                FileData fd = new FileData {
                    BaseFolder = BaseFolder,
                };
                fd.TryRemoveAll();
                return true;
            } catch (Exception exc) {
                errorList.Add(string.Format("{0}: {1}", BaseFolder, exc.Message));
                return false;
            }
        }
        public void AddSiteData() { }
        public void RemoveSiteData() { } // remove site-specific data is performed globally by removing the site data folder

        public bool ExportChunk(int chunk, SerializableList<SerializableFile> fileList, out object obj, bool SpecificType = false) {
            IDataProvider<KEYTYPE, OBJTYPE> iData = (IDataProvider<KEYTYPE, OBJTYPE>)this;
            int total;
            SerializableList<OBJTYPE> serList = new SerializableList<OBJTYPE>(iData.GetRecords(chunk * ChunkSize, ChunkSize, null, null, out total, SpecificType: SpecificType));
            obj = serList;
            int count = serList.Count();
            if (count == 0)
                obj = null;
            return (count >= ChunkSize);
        }
        public void ImportChunk(int chunk, SerializableList<SerializableFile> fileList, object obj) {
            IDataProvider<KEYTYPE, OBJTYPE> iData = (IDataProvider<KEYTYPE, OBJTYPE>)this;
            if (SiteIdentity > 0 || YetaWFManager.Manager.ImportChunksNonSiteSpecifics) {
                SerializableList<OBJTYPE> serList = (SerializableList<OBJTYPE>)obj;
                int total = serList.Count();
                if (total > 0) {
                    for (int processed = 0 ; processed < total ; ++processed) {
                        OBJTYPE item = serList[processed];
                        iData.Add(item);
                    }
                }
            }
        }
    }
}
