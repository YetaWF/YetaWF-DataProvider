/* Copyright © 2016 Softel vdm, Inc. - http://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.IO;
using System.Reflection;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Language;
using YetaWF.Core.Models;
using YetaWF.Core.Models.Attributes;
using YetaWF.Core.Support;
using YetaWF.Core.Support.Serializers;

namespace BigfootSQL {
    //TODO: Use ObjectSupport class if possible

    public interface ITypeConverter { object GetValue(Type fieldType, object value); }

    /// <summary>
    /// Object hydration class. Used primarily by the SqlHelper class to hydrate objects retreived from the database.
    /// The concept was originally inspired from the DotNetNuke object hydration process. Extensively expanded and transalted to C#
    ///
    /// Uses proper caching of objects maps to increase performance
    /// </summary>
    public class ObjectHelper
    {

        public ObjectHelper(List<LanguageData> languages) {
            Languages = languages;
        }

        public List<LanguageData> Languages;

        /// <summary>
        /// Generic cache class to cache object graphs and type converters
        /// </summary>
        public static class Cache
        {
            static Dictionary<string, object> _cache = new Dictionary<string, object>();


            public static bool Contains(String key) { return _cache.ContainsKey(key); }
            public static object GetValue(String key)
            {
                return (_cache.ContainsKey(key)) ? _cache[key] : null;
            }
            public static void Add(String key, object data)
            {
                if (_cache.ContainsKey(key))
                    _cache[key] = data;
                else
                    _cache.Add(key, data);
            }
            public static void Remove(string key)
            {
                if (_cache.ContainsKey(key)) _cache.Remove(key);
            }
        }

        /// <summary>
        /// Retreives a list of ITypeConverters in the system
        /// </summary>
        private Dictionary<String,ITypeConverter> GetAllConverters()
        {
            const string key = "TypeConverters";
            if (!Cache.Contains(key))
            {
                var types = new Dictionary<String, ITypeConverter>();
                Cache.Add(key,types);
            }
            return (Dictionary<String,ITypeConverter>)Cache.GetValue(key);
        }

        /// <summary>
        /// Gets the database value converter for a certain Type name
        /// </summary>
        /// <param name="typeName">The full name of the type e.g. System.String</param>
        /// <returns>A ITypeConverter object from the cache</returns>
        public ITypeConverter GetTypeConverter(String typeName)
        {
            typeName = typeName.ToLower();
            var converters = GetAllConverters();
            return (converters.ContainsKey(typeName)) ? converters[typeName] : null ;
        }

        /// <summary>
        /// Converts a database value to its object representation. Uses the TypeConverter cache to properly translate complex types
        /// </summary>
        /// <param name="fieldType">The Type of the field to convert to</param>
        /// <param name="value">The database value. e.g. the object from the data reader ordinal</param>
        /// <returns>A properly converted object</returns>
        public object GetValue(Type fieldType, object value)
        {
            // TODO: THIS IS HORRIBLE

            var typeName = fieldType.Name;
            object newValue = null;
            Type baseType = fieldType.BaseType;

            // Check if an empty value or an empty string
            if (value == null || value.ToString() == String.Empty)
                return newValue;

            if (fieldType.Equals(value.GetType()))
            {
                newValue = value;
            }
            else if (typeName == "Boolean")
            {
                newValue = (value.ToString() == "1" ||
                            value.ToString().ToLower() == "on" ||
                            value.ToString().ToLower() == "true" ||
                            value.ToString().ToLower() == "yes") ? true : false;
            }
            // Nullable types's name starts with nullable
            else if (typeName.StartsWith("Nullable"))
            {
                var typeFullName = fieldType.FullName;
                if (typeFullName.Contains("DateTime"))
                    newValue = Convert.ToDateTime(value);
                else if (typeFullName.Contains("TimeSpan"))
                    newValue = new TimeSpan(Convert.ToInt64(value));
                else if (typeFullName.Contains("Boolean"))
                    newValue = Convert.ToBoolean(value);
                else if (typeFullName.Contains("Int16"))
                    newValue = Convert.ToInt16(value);
                else if (typeFullName.Contains("Int32"))
                    newValue = Convert.ToInt32(value);
                else if (typeFullName.Contains("Integer"))
                    newValue = Convert.ToInt32(value);
                else if (fieldType.FullName.Contains("Int64"))
                    newValue = Convert.ToInt64(value);
                else if (fieldType.FullName.Contains("Decimal"))
                    newValue = Convert.ToDecimal(value);
                else if (typeFullName.Contains("Double"))
                    newValue = Convert.ToDouble(value);
                else if (typeFullName.Contains("Single"))
                    newValue = Convert.ToSingle(value);
                else if (typeFullName.Contains("UInt16"))
                    newValue = Convert.ToUInt16(value);
                else if (typeFullName.Contains("UInt32"))
                    newValue = Convert.ToUInt32(value);
                else if (typeFullName.Contains("UInt64"))
                    newValue = Convert.ToUInt64(value);
                else if (typeFullName.Contains("SByte"))
                    newValue = Convert.ToSByte(value);
                else if (typeFullName.Contains("System.Guid"))
                    newValue = new Guid(Convert.ToString(value));
                else
                    throw new InternalError("Unsupported type {0}", typeFullName);
            } else if (fieldType.FullName == "System.Guid") {
                newValue = new Guid(value.ToString());
            } else if (fieldType.FullName == "System.TimeSpan") {
                newValue = new TimeSpan(Convert.ToInt64(value));
            } else if (baseType != null && fieldType.BaseType == typeof(Enum)) {
                int intEnum;
                if (int.TryParse(value.ToString(), out intEnum))
                    newValue = intEnum;
                else
                {
                    try
                    {
                        newValue = Enum.Parse(fieldType, value.ToString());
                    }
                    catch (Exception)
                    {
                        newValue = Enum.ToObject(fieldType, value);

                    }
                }
            }
            else
            {
                // Try to get a specific type converter
                //  when no type converter is found then do a brute convert and ignore any errors that come up
                var converter = GetTypeConverter(fieldType.Name);
                if (converter != null)
                {
                    newValue = converter.GetValue(fieldType, value);
                }
                else
                {
                    try
                    {
                        newValue = Convert.ChangeType(value, fieldType);
                    }
                    catch (Exception){ }
                }
            }


            return newValue;

        }

        /// <summary>
        /// Fills a collection of objects using generics from a data reader object
        /// </summary>
        /// <typeparam name="T">The Type of the collection item object to fill</typeparam>
        /// <param name="reader">The reader object used to hydrate the collection</param>
        /// <returns>Collection of type of type T</returns>
        public List<T> FillCollection<T>(SqlDataReader reader)
        {
            var objCollection = new List<T>();

            try
            {
                while (reader.Read())
                {
                    objCollection.Add(CreateObject<T>(reader));
                }
            }
            finally
            {
                reader.Close();
            }

            return objCollection;
        }



        /// <summary>
        /// Fill a particular object
        /// </summary>
        /// <typeparam name="T">The type of object to fill</typeparam>
        /// <param name="dr">The reader object used to hydrate the object</param>
        /// <param name="manageDataReader">When set to true, closes the reader when finished</param>
        /// <returns>A hydrated object of type T</returns>
        public T FillObject<T>(IDataReader dr, bool manageDataReader = true)
        {
            var objFillObject = default(T);
            // Make sure the data reader has data
            if (manageDataReader && dr.Read() == false) return objFillObject;

            // Fill the object
            objFillObject = CreateObject<T>(dr);

            // Close the reader when in charge
            if (manageDataReader) dr.Close();

            // Return the filled object
            return objFillObject;
        }

        private T CreateObject<T>(IDataReader dr)
        {
            // Create a new instance of the object
            var objObject = Activator.CreateInstance<T>();

            // Check weather the object is a value type
            if (objObject.GetType().IsValueType)
            {
                var value = GetValue(objObject.GetType(), dr.GetValue(0));
                if (value != null) objObject = (T) value;
                return objObject;
            }

            FillObject(dr, objObject);

            return objObject;
        }

        public void FillObject(IDataReader dr, object container) {

            // Get the column names
            var columns = new Hashtable();
            for (var ci = 0 ; ci < dr.FieldCount ; ci++)
                columns[dr.GetName(ci)] = "";
            FillObject(dr, container, columns);
        }

        private void FillObject(IDataReader dr, object container, Hashtable columns, string prefix = "") {

            Type tpContainer = container.GetType();
            List<PropertyData> propData = ObjectSupport.GetPropertyData(tpContainer);
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave")) {
                    string colName = prefix + prop.Name;
                    if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        if (columns.ContainsKey(colName)) {
                            object val = dr[colName];
                            if (!(val is System.DBNull)) {
                                byte[] btes = (byte[]) val;
                                if (pi.PropertyType == typeof(byte[])) { // truly binary
                                    if (btes.Length > 0)
                                        pi.SetValue(container, btes, null);
                                } else {
                                    object data = new GeneralFormatter().Deserialize(btes);
                                    pi.SetValue(container, data, null);
                                }
                            }
                        }
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        MultiString ms = prop.GetPropertyValue<MultiString>(container);
                        foreach (var lang in Languages) {
                            string key = colName + "_" + lang.Id.Replace("-", "_");
                            if (columns.ContainsKey(key)) {
                                object value = dr[key];
                                if (!(value is System.DBNull)) {
                                    string s = (string) value;
                                    if (!string.IsNullOrWhiteSpace(s))
                                        ms[lang.Id] = s;
                                }
                            }
                        }
                    } else if (pi.PropertyType == typeof(Image)) {
                        if (columns.ContainsKey(colName)) {
                            byte[] btes = (byte[]) dr[colName];
                            if (btes.Length > 1) {
                                using (MemoryStream ms = new MemoryStream(btes)) {
                                    Image img = Image.FromStream(ms);
                                    pi.SetValue(container, img, null);
                                }
                            }
                        }
                    } else if (pi.PropertyType.IsClass && ComplexTypeInColumns(columns, colName + "_")) {
                        object propVal = pi.GetValue(container);
                        if (propVal != null)
                            FillObject(dr, propVal, columns, colName + "_");
                    } else {
                        if (columns.ContainsKey(prefix + pi.Name)) {
                            object value = dr[prefix + pi.Name];
                            pi.SetValue(container, GetValue(pi.PropertyType, value), BindingFlags.Default, null, null, null);
                        }
                    }
                }
            }
        }

        private bool ComplexTypeInColumns(Hashtable columns, string prefix) {
            foreach (var column in columns.Keys) {
                if (column.ToString().StartsWith(prefix))
                    return true;
            }
            return false;
        }

        private T CreateObject<T>(NameValueCollection values, string prefix, string suffix)
        {
            // Create a new instance of the object
            var objObject = Activator.CreateInstance<T>();
            // fill it
            return (T)CreateObject(objObject, values, prefix, suffix);
        }

        private object CreateObject(object objectToFill, NameValueCollection values, string prefix, string suffix)
        {
            // Make sure there are values to hydrate
            if (values == null || values.HasKeys() == false) return objectToFill;

            // Check weather the object is a value type
            if (objectToFill.GetType().IsValueType)
            {
                var value = GetValue(objectToFill.GetType(), values[0]);
                if (value != null) objectToFill = value;
                return objectToFill;
            }

            // Hydrate a complex type it
            //  Get the fields for the type
            List<PropertyInfo> props = GetProperties(objectToFill);
            foreach (var prop in props)
            {
                // Get the fieldname
                string fieldName = prop.Name.ToUpperInvariant();

                // Loop through the values
                foreach (string formKey in values.Keys)
                {
                    if ((prefix + fieldName + suffix) == formKey.ToUpperInvariant())
                    {
                        prop.SetValue(objectToFill, GetValue(prop.PropertyType, values[formKey]), BindingFlags.Default, null, null, null);
                        // Go to the next item
                        break;
                    }
                }
            }
            return objectToFill;
        }

        /// <summary>
        /// Get the properties to hydrate for an object.
        /// </summary>
        /// <param name="obj">Object to use to hydrate</param>
        /// <returns>A list of properties</returns>
        private List<PropertyInfo> GetProperties(object obj)
        {
            List<PropertyInfo> properties = new List<PropertyInfo>();
            Type type = obj.GetType();
            List<PropertyInfo> props = ObjectSupport.GetProperties(type);
            foreach (PropertyInfo p in props)
            {
                if (!p.CanRead || !p.CanWrite)
                    continue;
                if (Attribute.GetCustomAttribute(p, typeof(DontSaveAttribute)) != null)
                    continue;
                if (Attribute.GetCustomAttribute(p, typeof(Data_CalculatedProperty)) != null)
                    continue;
                properties.Add(p);
            }
            return properties;
        }

    }
}
