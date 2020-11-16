/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Language;
using YetaWF.Core.Models;
using YetaWF.Core.Packages;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.SQLGeneric {

    /// <summary>
    /// This abstract class is the base class for all SQL low-level data providers.
    /// </summary>
    public abstract class SQLGenericBase : IDisposable {

        /// <summary>
        /// Defines the default key used in appsettings.json.
        /// </summary>
        public const string DefaultString = "Default";

        /// <summary>
        /// Defines the column name used to associate a site with a data record. The __Site column contains the site ID, or 0 if there is no associated site.
        /// Not all tables use the __Site column.
        /// </summary>
        public static string SiteColumn { get; } = "__Site";
        /// <summary>
        /// Defines the column name of the identity column used in tables. Not all tables use an identity column.
        /// </summary>
        public static string IdentityColumn { get; } = "Identity";
        /// <summary>
        /// Defines the column name in subtables to connect a subtable and its records to the main table.
        /// The __Key column in a subtable contains the identity column used in the main table, used to join record data across tables.
        /// </summary>
        public static string SubTableKeyColumn  { get; } = "__Key";

        /// <summary>
        /// A dictionary of options and optional parameters as provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider was created.
        /// </summary>
        public Dictionary<string, object> Options { get; private set; }

        /// <summary>
        /// The package implementing the data provider.
        /// </summary>
        public Package Package { get; private set; }

        /// <summary>
        /// The section in AppSettings.json, where SQL connection string, database owner, etc. are located.
        /// WebConfigArea is normally not specified and all connection information is derived from the AppSettings.json section that corresponds to the table name used by the data provider.
        /// This can be overridden by passing an optional WebConfigArea parameter to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.
        /// </summary>
        /// <remarks>This is not used by application data providers. Only the YetaWF.DataProvider.ModuleDefinitionDataProvider uses this feature.</remarks>
        public string? WebConfigArea { get; private set; }

        /// <summary>
        /// The dataset provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider was created.
        /// </summary>
        public string Dataset { get; protected set; }
        /// <summary>
        /// The database used by this data provider. This information is extracted from the SQL connection string.
        /// </summary>
        public string Database { get; protected set; } = null!;
        /// <summary>
        /// The site identity provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider was created.
        ///
        /// This may be 0 if no specific site is associated with the data provider.
        /// </summary>
        public int SiteIdentity { get; private set; }
        /// <summary>
        /// The initial value of the identity seed. The default value is defined by YetaWF.Core.DataProvider.DataProviderImpl.IDENTITY_SEED, but this can be overridden by passing an
        /// optional IdentitySeed parameter to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.
        /// </summary>
        public int IdentitySeed { get; private set; }
        /// <summary>
        /// Defines whether the data is cacheable.
        /// This corresponds to the Cacheable parameter of the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method.
        /// </summary>
        public bool Cacheable { get; private set; }
        /// <summary>
        /// Defines whether logging is wanted for the data provider. The default value is false, but this can be overridden by passing an
        /// optional Logging parameter to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.
        /// </summary>
        public bool Logging { get; private set; }
        /// <summary>
        /// Defines whether language support (for YetaWF.Core.Models.MultiString) is wanted for the data provider. The default is true. This can be overridden by passing an
        /// optional NoLanguages parameter to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.
        /// </summary>
        public bool NoLanguages { get; private set; }
        /// <summary>
        /// Defines the languages supported by the data provider. If NoLanguages is true, no language data is available.
        /// Otherwise, the languages supported are identical to collection of active languages defined by YetaWF.Core.Models.MultiString.Languages.
        /// </summary>
        public List<LanguageData> Languages { get; private set; }

        /// <summary>
        /// An optional callback which is called whenever an object is retrieved to update some properties.
        /// </summary>
        /// <remarks>
        /// Properties that are derived from other property values are considered "calculated properties". This callback
        /// is called after retrieving an object to update these properties.
        ///
        /// This callback is typically set by the data provider itself, in its constructor or as the data provider is being created.
        /// </remarks>
        protected Func<string, Task<string>>? CalculatedPropertyCallbackAsync { get; set; }


        /// <summary>
        /// Defines whether the model defines a secondary key.
        /// </summary>
        public bool HasKey2 { get; protected set; }
        /// <summary>
        /// The column name of the primary key.
        /// </summary>
        /// <remarks>If a primary key has not been defined in the model, an exception occurs when this property is retrieved.</remarks>
        public string Key1Name { get { return GetKey1Name(Dataset, GetPropertyData()); } }
        /// <summary>
        /// The column name of the secondary key.
        /// </summary>
        /// <remarks>If a secondary key has not been defined in the model, an exception occurs when this property is retrieved.</remarks>
        public string Key2Name { get { return GetKey2Name(Dataset, GetPropertyData()); } }
        /// <summary>
        /// The column name of the identity column.
        /// </summary>
        /// <remarks>If no identity column is defined for the specified table, an empty string is returned.</remarks>
        public string IdentityName { get { return GetIdentityName(Dataset, GetPropertyData()); } }

        /// <summary>
        /// Returns the identity column name or the default identity column name for the current object type.
        /// </summary>
        /// <remarks>This should only be used for objects that are known to have an identity column.</remarks>
        protected string IdentityNameOrDefault {
            get {
                if (string.IsNullOrWhiteSpace(_identityOrDefault))
                    _identityOrDefault = GetIdentityName(Dataset, GetPropertyData());
                if (string.IsNullOrWhiteSpace(_identityOrDefault))
                    _identityOrDefault = IdentityColumn;
                return _identityOrDefault;
            }
        }
        private string? _identityOrDefault;

        /// <summary>
        /// Retrieves the property information for the model used.
        /// </summary>
        /// <returns>List of property information.</returns>
        protected abstract List<PropertyData> GetPropertyData();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="options">A dictionary of options and optional parameters as provided to the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method when the data provider is created.</param>
        /// <param name="HasKey2">Defines whether the object has a secondary key.</param>
        /// <remarks>
        /// Data providers are instantiated when the YetaWF.Core.DataProvider.DataProviderImpl.MakeDataProvider method is called, usually by an application data provider.
        ///
        /// For debugging purposes, instances of this class are tracked using the DisposableTracker class.
        /// </remarks>
        protected SQLGenericBase(Dictionary<string, object> options, bool HasKey2 = false) {

            this.HasKey2 = HasKey2;

            Options = options;
            if (!Options.ContainsKey(nameof(Package)) || !(Options[nameof(Package)] is Package))
                throw new InternalError($"No Package for data provider {GetType().FullName}");
            Package = (Package)Options[nameof(Package)];
            if (!Options.ContainsKey(nameof(Dataset)) || string.IsNullOrWhiteSpace((string)Options[nameof(Dataset)]))
                throw new InternalError($"No Dataset for data provider {GetType().FullName}");
            Dataset = (string)Options[nameof(Dataset)];
            if (Options.ContainsKey(nameof(SiteIdentity)) && Options[nameof(SiteIdentity)] is int)
                SiteIdentity = Convert.ToInt32(Options[nameof(SiteIdentity)]);
            if (Options.ContainsKey(nameof(IdentitySeed)) && Options[nameof(IdentitySeed)] is int)
                IdentitySeed = Convert.ToInt32(Options[nameof(IdentitySeed)]);
            else
                IdentitySeed = DataProviderImpl.IDENTITY_SEED;
            if (Options.ContainsKey(nameof(Cacheable)) && Options[nameof(Cacheable)] is bool)
                Cacheable = Convert.ToBoolean(Options[nameof(Cacheable)]);
            if (Options.ContainsKey(nameof(Logging)) && Options[nameof(Logging)] is bool)
                Logging = Convert.ToBoolean(Options[nameof(Logging)]);
            else
                Logging = true;
            if (Options.ContainsKey(nameof(NoLanguages)) && Options[nameof(NoLanguages)] is bool)
                NoLanguages = Convert.ToBoolean(Options[nameof(NoLanguages)]);

            if (Options.ContainsKey("WebConfigArea"))
                WebConfigArea = (string)Options["WebConfigArea"];

            if (NoLanguages)
                Languages = new List<LanguageData>();
            else {
                Languages = MultiString.Languages;
                if (Languages.Count == 0) throw new InternalError("We need Languages");
            }
            DisposableTracker.AddObject(this);
        }
        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose() { Dispose(true); }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="disposing">true to close the database connection and release the DisposableTracker reference count, false otherwise.</param>
        protected virtual void Dispose(bool disposing) {
            if (disposing)
                DisposableTracker.RemoveObject(this);
        }

        /// <summary>
        /// Returns the primary key's column name.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        /// <param name="propertyData">The collection of property information.</param>
        /// <returns> Returns the primary key's column name.</returns>
        /// <remarks>
        /// A primary key is defined in a model by decorating a property with the YetaWF.Core.DataProvider.Attributes.Data_PrimaryKey attribute.
        /// If no primary key is defined for the specified table, an exception occurs.
        /// </remarks>
        protected string GetKey1Name(string tableName, List<PropertyData> propertyData) {
            if (_key1Name == null) {
                // find primary key
                foreach (PropertyData prop in propertyData) {
                    if (prop.HasAttribute(Data_PrimaryKey.AttributeName)) {
                        _key1Name = prop.Name;
                        return prop.Name;
                    }
                }
                throw new InternalError("Primary key not defined in table {0}", tableName);
            }
            return _key1Name;
        }
        private string? _key1Name;

        /// <summary>
        /// Returns the secondary key's column name.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        /// <param name="propertyData">The collection of property information.</param>
        /// <returns> Returns the secondary key's column name.</returns>
        /// <remarks>
        /// A secondary key is defined in a model by decorating a property with the YetaWF.Core.DataProvider.Attributes.Data_PrimaryKey2 attribute.
        /// If no secondary key is defined for the specified table, an exception occurs.
        /// </remarks>
        protected string GetKey2Name(string tableName, List<PropertyData> propertyData) {
            if (_key2Name == null) {
                // find primary key
                foreach (PropertyData prop in propertyData) {
                    if (prop.HasAttribute(Data_PrimaryKey2.AttributeName)) {
                        _key2Name = prop.Name;
                        return prop.Name;
                    }
                }
                throw new InternalError("Second primary key not defined in table {0}", tableName);
            }
            return _key2Name;
        }
        private string? _key2Name;

        /// <summary>
        /// Returns the identity column name.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        /// <param name="propertyData">The collection of property information.</param>
        /// <returns>Returns the identity column name.</returns>
        /// <remarks>
        /// An identity column is defined in a model by decorating a property with the YetaWF.Core.DataProvider.Attributes.Data_Identity attribute.
        /// If no identity column is defined for the specified table, an empty string is returned.
        /// </remarks>
        protected string GetIdentityName(string tableName, List<PropertyData> propertyData) {
            if (_identityName == null) {
                // find identity
                foreach (PropertyData prop in propertyData) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        _identityName = prop.Name;
                        return _identityName;
                    }
                }
                _identityName = string.Empty;
            }
            return _identityName;
        }
        private string? _identityName;

        /// <summary>
        /// Returns whether the specified identity name string <paramref name="identityName"/> is a valid identity name.
        /// </summary>
        /// <param name="identityName">A string.</param>
        /// <returns>Returns whether the specified identity name strin <paramref name="identityName"/> is a valid identity name.</returns>
        protected bool HasIdentity(string identityName) {
            return !string.IsNullOrWhiteSpace(identityName);
        }

        /// <summary>
        /// Tests whether a given type is a simple type that can be stored in one column.
        /// </summary>
        /// <param name="tp">The type to test.</param>
        /// <returns>Returns true if the type is a simple type that can be stored in one column, false otherwise.</returns>
        public static bool TryGetDataType(Type tp) {
            if (tp == typeof(DateTime) || tp == typeof(DateTime?))
                return true;
            else if (tp == typeof(TimeSpan) || tp == typeof(TimeSpan?))
                return true;
            else if (tp == typeof(decimal) || tp == typeof(decimal?))
                return true;
            else if (tp == typeof(bool) || tp == typeof(bool?))
                return true;
            else if (tp == typeof(System.Guid) || tp == typeof(System.Guid?))
                return true;
            else if (tp == typeof(Image))
                return true;
            else if (tp == typeof(int) || tp == typeof(int?))
                return true;
            else if (tp == typeof(long) || tp == typeof(long?))
                return true;
            else if (tp == typeof(Single) || tp == typeof(Single?))
                return true;
            else if (tp == typeof(string))
                return true;
            else if (tp.IsEnum)
                return true;
            return false;
        }

        // SORTS, FILTERS
        // SORTS, FILTERS
        // SORTS, FILTERS

        /// <summary>
        /// Normalizes filters and updates column names for constructed names (as used in MultiString).
        /// </summary>
        /// <param name="type">The target object type for which filters are normalized.</param>
        /// <param name="filters">The filters to be normalized, may be null.</param>
        /// <returns>Returns normalized filters.</returns>
        protected List<DataProviderFilterInfo>? NormalizeFilter(Type type, List<DataProviderFilterInfo>? filters) {
            if (filters == null)
                return null;
            filters = (from f in filters select new DataProviderFilterInfo(f)).ToList();// copy list
            foreach (DataProviderFilterInfo f in filters) {
                if (f.Field != null && f.Field.Length > 0 && char.IsLetter(f.Field[0])) // don't replace in [column]...
                    f.Field = f.Field.Replace(".", "_");
            }
            DataProviderFilterInfo.NormalizeFilters(type, filters);
            foreach (DataProviderFilterInfo filter in filters) {
                if (filter.Filters != null)
                    filter.Filters = NormalizeFilter(type, filter.Filters);
                else if (!string.IsNullOrWhiteSpace(filter.Field))
                    filter.Field = NormalizeFilter(type, filter);
            }
            return filters;
        }
        private string NormalizeFilter(Type type, DataProviderFilterInfo filter) {
            PropertyData? propData = ObjectSupport.TryGetPropertyData(type, filter.Field!);
            if (propData == null)
                return filter.Field!; // could be a composite field, like Event.ImplementingAssembly
            if (propData.PropInfo.PropertyType == typeof(MultiString)) {
                MultiString ms = new MultiString(filter.ValueAsString);
                filter.Value = ms.ToString();
                return ColumnFromPropertyWithLanguage(MultiString.ActiveLanguage, filter.Field!);
            }
            return propData.ColumnName;
        }
        /// <summary>
        /// Normalizes sort filters.
        /// </summary>
        /// <param name="type">The target object type for which filters are normalized.</param>
        /// <param name="sorts">The filters to be normalized, may be null.</param>
        /// <returns>Returns normalized filters.</returns>
        protected List<DataProviderSortInfo>? NormalizeSort(Type type, List<DataProviderSortInfo>? sorts) {
            if (sorts == null)
                return null;
            sorts = (from s in sorts select new DataProviderSortInfo(s)).ToList();// copy list
            foreach (DataProviderSortInfo sort in sorts) {
                if (sort.Field != null && sort.Field.Length > 0 && char.IsLetter(sort.Field[0])) // don't replace in [column]...
                    sort.Field = sort.Field.Replace(".", "_");
            }
            foreach (DataProviderSortInfo sort in sorts) {
                PropertyData? propData = ObjectSupport.TryGetPropertyData(type, sort.Field);
                if (propData != null) {
                    if (propData.PropInfo.PropertyType == typeof(MultiString))
                        sort.Field = ColumnFromPropertyWithLanguage(MultiString.ActiveLanguage, sort.Field);
                }
            }
            return sorts;
        }
        /// <summary>
        /// Returns a column name based on language id.
        /// </summary>
        /// <param name="langId">The language id.</param>
        /// <param name="field">The original column name.</param>
        /// <returns>Returns a column name based on language id.</returns>
        public static string ColumnFromPropertyWithLanguage(string langId, string field) {
            return field + "_" + langId.Replace("-", "_");
        }
        /// <summary>
        /// Returns the suffix appended to language dependent columns using the active language.
        /// </summary>
        /// <returns>Returns the suffix appended to language dependent columns using the active language.</returns>
        public static string GetLanguageSuffix() {
            return ColumnFromPropertyWithLanguage(MultiString.ActiveLanguage, "");
        }
    }
}
