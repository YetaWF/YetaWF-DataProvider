/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Npgsql;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Language;
using YetaWF.Core.Models;
using YetaWF.Core.Support;
using YetaWF.DataProvider.SQLGeneric;

namespace YetaWF.DataProvider.PostgreSQL {

    internal partial class SQLGen {

        internal bool MakeProceduresAndFunctionsWithBaseType(string dbName, string schema, string baseDataset, string dataset, string key1Name, string identityName, List<PropertyData> basePropData, List<PropertyData> propData, Type baseType, Type type, string DerivedDataTableName, string DerivedDataTypeName, string DerivedAssemblyName) {
            //$$$
            return true;
        }

        // http://www.sqlines.com/postgresql/how-to/return_result_set_from_stored_procedure

        internal async Task<bool> MakeProceduresAndFunctionsAsync(string dbName, string schema, string dataset, string key1Name, string key2Name, string identityName, List<PropertyData> propData, Type objType, int siteIdentity,
                Func<string, Task<string>> calculatedPropertyCallbackAsync) {

            SQLBuilder sb = new SQLBuilder();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = sb.GetTable(dbName, schema, dataset);
            List<SubTableInfo> subTables = GetSubTables(dataset, propData);

            Column colKey1 = SQLGenericManagerCache.GetCachedColumn(dbName, schema, dataset, key1Name);
            string typeKey1 = GetDataTypeArgumentString(colKey1);

            // GET
            // GET
            // GET

            sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Get"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__Get""(Key1Val {typeKey1},");

            if (!string.IsNullOrWhiteSpace(key2Name)) {
                Column colKey2 = SQLGenericManagerCache.GetCachedColumn(dbName, schema, dataset, key2Name);
                string typeKey2 = GetDataTypeArgumentString(colKey2);
                sb.Append($@"Key2Val {typeKey2},");
            }
            if (siteIdentity > 0)
                sb.Append($@"SiteIdentityVal integer,");

            sb.RemoveLastComma();
            sb.Append($@")");


            sb.Append($@"
    RETURNS SETOF refcursor
    LANGUAGE 'plpgsql'
AS $$
DECLARE");

            for (int refcnt = 0; refcnt < subTables.Count + 1; ++refcnt)
                sb.Append($@"
        ref{refcnt} refcursor;");

            sb.Append($@"
BEGIN
OPEN ref0 FOR
    SELECT *");
            if (calculatedPropertyCallbackAsync != null) sb.Append(await CalculatedPropertiesAsync(objType, calculatedPropertyCallbackAsync));

            sb.Append($@"
    FROM {fullTableName}
    WHERE {SQLBuilder.WrapIdentifier(key1Name)} = Key1Val");
            if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND {SQLBuilder.WrapIdentifier(key2Name)} = Key2Val");
            if (siteIdentity > 0) sb.Append($@" AND {SQLBuilder.WrapIdentifier(SQLGenericBase.SiteColumn)} = SiteIdentityVal");

            sb.Append($@"
    FETCH FIRST 1 ROWS ONLY;
    RETURN NEXT ref0           --- result set
;");

            int refCount = 1;
            foreach (SubTableInfo subTable in subTables) {
                sb.Add($@"
OPEN ref{refCount} FOR
    SELECT * FROM {sb.BuildFullTableName(dbName, schema, subTable.Name)}
    INNER JOIN {sb.BuildFullTableName(dbName, schema, dataset)} ON {sb.BuildFullColumnName(dataset, identityName)} = {sb.BuildFullColumnName(subTable.Name, SQLGenericBase.SubTableKeyColumn)}
    WHERE {SQLBuilder.WrapIdentifier(key1Name)} = Key1Val");
                if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND {SQLBuilder.WrapIdentifier(key2Name)} = Key2Val");
                if (siteIdentity > 0) sb.Append($@" AND {SQLBuilder.WrapIdentifier(SQLGenericBase.SiteColumn)} = SiteIdentityVal");

                sb.Add($@";
    RETURN NEXT ref{refCount}            --- result set
;");
                ++refCount;
            }
            sb.Append($@"
END;
$$;");

            // ADD
            // ADD
            // ADD

            sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Add"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__Add""({GetArgumentNameList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");

            sb.RemoveLastComma();
            sb.Append($@")");

            if (HasIdentity(identityName)) {

                sb.Append($@"
    RETURNS integer");
            } else {
                sb.Append($@"
    RETURNS void");
            }

            sb.Append($@"
    LANGUAGE 'plpgsql'
AS $$");
            if (HasIdentity(identityName)) {

                sb.Append($@"
DECLARE __IDENTITY integer;");
            }

            sb.Append($@"
BEGIN
    INSERT INTO {fullTableName} ({GetColumnNameList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)})
    VALUES ({GetValueNameList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)})");

            if (HasIdentity(identityName)) {

                sb.Append($@"
    RETURNING {SQLBuilder.WrapIdentifier(identityName)} INTO __IDENTITY");
            }
            sb.Append($@"
;");

            foreach (SubTableInfo subTable in subTables) {
                sb.Add($@"
    INSERT INTO {sb.BuildFullTableName(dbName, schema, subTable.Name)} ({GetColumnNameList(dbName, schema, subTable.Name, propData, objType, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)})
    VALUES ({GetValueNameList(dbName, schema, subTable.Name, propData, objType, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)})
;");
            }

            sb.Append($@"

    SELECT __IDENTITY; --result set
END;
$$;");

            // REMOVE
            // REMOVE
            // REMOVE

            sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Remove"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__Remove""(Key1Val {typeKey1},");

            if (!string.IsNullOrWhiteSpace(key2Name)) {
                Column colKey2 = SQLGenericManagerCache.GetCachedColumn(dbName, schema, dataset, key2Name);
                string typeKey2 = GetDataTypeArgumentString(colKey2);
                sb.Append($@"Key2Val {typeKey2},");
            }
            if (siteIdentity > 0)
                sb.Append($@"SiteIdentityVal integer,");

            sb.RemoveLastComma();
            sb.Append($@")
    RETURNS integer
    LANGUAGE 'plpgsql'
AS $$
    DECLARE __TOTAL integer;");

            if (subTables.Count == 0) {

                sb.Append($@"
BEGIN
    DELETE FROM {fullTableName}
    WHERE {SQLBuilder.WrapIdentifier(key1Name)} = Key1Val");
                if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND {SQLBuilder.WrapIdentifier(key2Name)} = Key2Val");
                if (siteIdentity > 0) sb.Append($@" AND {SQLBuilder.WrapIdentifier(SQLGenericBase.SiteColumn)} = SiteIdentityVal");

                sb.Append($@"
;");

            } else {

                sb.Append($@"
    DECLARE __IDENTITY integer;
BEGIN
    SELECT {SQLBuilder.WrapIdentifier(identityName)}  INTO __IDENTITY FROM {fullTableName}
    WHERE {SQLBuilder.WrapIdentifier(key1Name)} = Key1Val");
    if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND {SQLBuilder.WrapIdentifier(key2Name)} = Key2Val");
    if (siteIdentity > 0) sb.Append($@" AND {SQLBuilder.WrapIdentifier(SQLGenericBase.SiteColumn)} = SiteIdentityVal");
                sb.Append($@"
;");

                foreach (SubTableInfo subTable in subTables) {
                    sb.Add($@"
    DELETE FROM {sb.BuildFullTableName(dbName, schema, subTable.Name)} WHERE {sb.BuildFullColumnName(subTable.Name, SQLGenericBase.SubTableKeyColumn)} = __IDENTITY
;");
                }
                sb.Add($@"
    DELETE FROM {fullTableName} WHERE {SQLBuilder.WrapIdentifier(identityName)} = __IDENTITY
;");
                
            }

            sb.Append($@"
    GET DIAGNOSTICS __TOTAL = ROW_COUNT
;
    SELECT __TOTAL; --- result set
END;
$$;");


            // Add to database
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = Conn;
                cmd.CommandText = sb.ToString();
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }

            return true;
        }

        internal class SubTableInfo {
            public string Name { get; set; }
            public Type Type { get; set; }
            public PropertyInfo PropInfo { get; set; } // the container's property that holds this subtable
        }

        internal List<SubTableInfo> GetSubTables(string tableName, List<PropertyData> propData) {
            SQLBuilder sb = new SQLBuilder();
            List<SubTableInfo> list = new List<SubTableInfo>();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        ; // nothing
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        ; // nothing
                    } else if (pi.PropertyType == typeof(Image)) {
                        ; // nothing
                    } else if (pi.PropertyType == typeof(TimeSpan)) {
                        ; // nothing
                    } else if (SQLGenericBase.TryGetDataType(pi.PropertyType)) {
                        ; // nothing
                    } else if (pi.PropertyType.IsClass && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // enumerated type -> subtable
                        Type subType = pi.PropertyType.GetInterfaces().Where(t => t.IsGenericType == true && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                                .Select(t => t.GetGenericArguments()[0]).FirstOrDefault();
                        string subTableName = sb.BuildFullTableName(tableName + "_" + pi.Name);
                        list.Add(new SubTableInfo {
                            Name = subTableName,
                            Type = subType,
                            PropInfo = pi,
                        });
                    }
                }
            }
            return list;
        }

        internal async Task<string> CalculatedPropertiesAsync(Type objType, Func<string, Task<string>> calculatedPropertyCallbackAsync) {
            if (calculatedPropertyCallbackAsync == null) return null;
            SQLBuilder sb = new SQLBuilder();
            List<PropertyData> props = ObjectSupport.GetPropertyData(objType);
            props = (from p in props where p.CalculatedProperty select p).ToList();
            foreach (PropertyData prop in props) {
                string calcProp = await calculatedPropertyCallbackAsync(prop.Name);
                sb.Add($@", 
          ({calcProp}) AS {SQLBuilder.WrapIdentifier(prop.Name)}");
            }
            return sb.ToString();
        }

        internal string GetArgumentNameList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, string Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            return GetColumnFormattedList(
                (prefix, prop) => {
                    Column col = SQLGenericManagerCache.GetCachedColumn(dbName, schema, dataset, prop.ColumnName);
                    string colType = GetDataTypeArgumentString(col);
                    return $"arg{prefix}{prop.Name} {colType},";
                },
                (prefix, prop) => {
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sb = new StringBuilder();
                    foreach (LanguageData lang in Languages)
                        sb.Append($"arg{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name)} character varying,");
                    return sb.ToString();
                },
                (prefix, name) => {
                    string colType = "character varying";
                    if (name == SQLGenericBase.SiteColumn || name == SQLGenericBase.SubTableKeyColumn)
                        colType = "integer";
                    return $"arg{prefix}{name} {colType},";
                },
                dbName, schema, dataset, propData, tpContainer, Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal string GetColumnNameList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, string Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            return GetColumnFormattedList(
                (prefix, prop) => { return $"{SQLBuilder.WrapIdentifier($"{prefix}{prop.ColumnName}")},"; },
                (prefix, prop) => {
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sb = new StringBuilder();
                    foreach (LanguageData lang in Languages)
                        sb.Append($"{SQLBuilder.WrapIdentifier($"{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name)}")}");
                    return sb.ToString();
                },
                (prefix, name) => { return $"{SQLBuilder.WrapIdentifier($"{prefix}{name}")},"; },
                dbName, schema, dataset, propData, tpContainer, Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal string GetValueNameList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, string Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            return GetColumnFormattedList(
                (prefix, prop) => { return $"arg{prefix}{prop.Name},"; },
                (prefix, prop) => {
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sb = new StringBuilder();
                    foreach (LanguageData lang in Languages)
                        sb.Append($"arg{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name)} character varying,");
                    return sb.ToString();
                },
                (prefix, name) => {
                    if (name == SQLGenericBase.SubTableKeyColumn)
                        return $"__IDENTITY,";
                    return $"arg{prefix}{name},"; 
                },
                dbName, schema, dataset, propData, tpContainer, Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal static string GetColumnFormattedList(Func<string, PropertyData, string> fmt, Func<string, PropertyData, string> fmtLanguage, Func<string, string, string> fmtPredef,
                string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, string Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            SQLBuilder sb = new SQLBuilder();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    string colName = prop.ColumnName;
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        sb.Add(fmt(Prefix, prop));
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        sb.Add(fmtLanguage(Prefix, prop));
                    } else if (pi.PropertyType == typeof(Image)) {
                        sb.Add(fmt(Prefix, prop));
                    } else if (SQLGenericBase.TryGetDataType(pi.PropertyType)) {
                        sb.Add(fmt(Prefix, prop));
                    } else if (pi.PropertyType.IsClass /* && propmmd.Model != null*/ && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // This is a enumerated type, so we have to create separate values using this table's identity column as a link
                        ; // these values are added as a subtable
                    } else if (pi.PropertyType.IsClass /*&& propmmd.Model != null*/) {
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(pi.PropertyType);
                        string args = GetColumnFormattedList(fmt, fmtLanguage, fmtPredef, dbName, schema, dataset, subPropData, pi.PropertyType, Prefix + colName + "_", false, SiteSpecific: false);
                        if (!string.IsNullOrWhiteSpace(args))
                            sb.Add(args);
                    } else
                        throw new InternalError($"Unknown property type {pi.PropertyType.FullName} used in class {tpContainer.FullName}, property {colName}");
                }
            }
            if (SiteSpecific)
                sb.Add(fmtPredef(Prefix, "SiteIdentityVal"));
            if (WithDerivedInfo) {
                sb.Add(fmtPredef(Prefix, "DerivedDataTableName"));
                sb.Add(fmtPredef(Prefix, "DerivedDataType"));
                sb.Add(fmtPredef(Prefix, "DerivedAssemblyName"));
            }
            if (SubTable)
                sb.Add(fmtPredef(Prefix, SQLGenericBase.SubTableKeyColumn));
            if (TopMost || SubTable)
                sb.RemoveLastComma();
            return sb.ToString();
        }
    }
}