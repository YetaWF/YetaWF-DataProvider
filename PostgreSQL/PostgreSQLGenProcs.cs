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

        public const string ValSiteIdentity = "valSiteIdentity"; // sproc argument with site identity

        // http://www.sqlines.com/postgresql/how-to/return_result_set_from_stored_procedure

        internal async Task<bool> MakeFunctionsAsync(string dbName, string schema, string dataset, string key1Name, string key2Name, string identityName, List<PropertyData> propData, Type objType, int siteIdentity,
                Func<string, Task<string>> calculatedPropertyCallbackAsync) {

            SQLManager sqlManager = new SQLManager();
            SQLBuilder sb = new SQLBuilder();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = sb.GetTable(dbName, schema, dataset);
            List<SubTableInfo> subTables = GetSubTables(dataset, propData);

            Column colKey1 = sqlManager.GetColumn(Conn, dbName, schema, dataset, key1Name);
            string typeKey1 = GetDataTypeArgumentString(colKey1);

            Column colKey2 = null;
            string typeKey2 = null;
            if (!string.IsNullOrWhiteSpace(key2Name)) {
                colKey2 = sqlManager.GetColumn(Conn, dbName, schema, dataset, key2Name);
                typeKey2 = GetDataTypeArgumentString(colKey2);
            }

            // GET
            // GET
            // GET

            sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Get"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__Get""(""Key1Val"" {typeKey1},");
            if (!string.IsNullOrWhiteSpace(key2Name))
                sb.Append($@"""Key2Val"" {typeKey2},");
            if (siteIdentity > 0)
                sb.Append($@"""{SQLGen.ValSiteIdentity}"" integer,");

            sb.RemoveLastComma();
            sb.Append($@")");

            sb.Append($@"
    RETURNS SETOF ""{schema}"".""{dataset}_T""
    LANGUAGE 'plpgsql'
AS $$
BEGIN
    RETURN QUERY (
        SELECT {GetColumnNameList(dbName, schema, dataset, propData, objType, Add: false, Prefix: null, TopMost: false, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");
            if (calculatedPropertyCallbackAsync != null) sb.Append(await CalculatedPropertiesAsync(objType, calculatedPropertyCallbackAsync));

            foreach (SubTableInfo subTable in subTables) {

                List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);

                sb.Append($@"
            (
                SELECT ARRAY_AGG((");

                sb.Append(GetColumnNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Add: false, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true));
                sb.RemoveLastComma();
                sb.Append($@")::");

                if (subPropData.Count == 1) {
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, subTable.Name, subPropData[0].ColumnName);
                    string colType = GetDataTypeArgumentString(col);
                    sb.Append(colType);
                } else {
                    sb.Append($@"""{subTable.Name}_T""");
                }

                sb.Append($@")
                FROM ""{schema}"".""{subTable.Name}""
                WHERE ""{schema}"".""{subTable.Name}"".""{SQLGenericBase.SubTableKeyColumn}"" = ""{schema}"".""{dataset}"".""{GetIdentityNameOrDefault(identityName)}""
            ) AS ""{subTable.PropInfo.Name}"",");

            }

            sb.RemoveLastComma();

            sb.Append($@"
        FROM {fullTableName}
        WHERE ""{key1Name}"" = ""Key1Val""");
            if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND ""{key2Name}"" = ""Key2Val""");
            if (siteIdentity > 0) sb.Append($@" AND ""{SQLGenericBase.SiteColumn}"" = ""{SQLGen.ValSiteIdentity}""");

            sb.Append($@"
        LIMIT 1    --- result set
    )
;
END;
$$;");

            // GET BY IDENTITY
            // GET BY IDENTITY
            // GET BY IDENTITY

            if (HasIdentity(identityName)) {

                sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__GetByIdentity"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__GetByIdentity""(""valIdentity"" integer)
    RETURNS SETOF ""{schema}"".""{dataset}_T""
    LANGUAGE 'plpgsql'
AS $$
BEGIN
    RETURN QUERY (
        SELECT {GetColumnNameList(dbName, schema, dataset, propData, objType, Add: false, Prefix: null, TopMost: false, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");
                if (calculatedPropertyCallbackAsync != null) sb.Append(await CalculatedPropertiesAsync(objType, calculatedPropertyCallbackAsync));

                foreach (SubTableInfo subTable in subTables) {
                    List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);

                    sb.Append($@"
            (
                SELECT ARRAY_AGG((");

                    sb.Append(GetColumnNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Add: false, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true));
                    sb.RemoveLastComma();
                    sb.Append($@")::");

                    if (subPropData.Count == 1) {
                        Column col = sqlManager.GetColumn(Conn, dbName, schema, subTable.Name, subPropData[0].ColumnName);
                        string colType = GetDataTypeArgumentString(col);
                        sb.Append(colType);
                    } else {
                        sb.Append($@"""{subTable.Name}_T""");
                    }

                    sb.Append($@")
                FROM ""{schema}"".""{subTable.Name}""
                WHERE ""{schema}"".""{subTable.Name}"".""{SQLGenericBase.SubTableKeyColumn}"" = ""{schema}"".""{dataset}"".""{GetIdentityNameOrDefault(identityName)}""
            ) AS ""{subTable.PropInfo.Name}"",");

                }
                sb.RemoveLastComma();

                sb.Append($@"
        FROM {fullTableName}
        WHERE ""{identityName}"" = ""valIdentity""");

                sb.Append($@"
        LIMIT 1    --- result set
    )
;
END;
$$;");
            }

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
AS $$
DECLARE
    __TOTAL integer;
    __COUNT integer;");
            if (HasIdentity(identityName)) {

                sb.Append($@"
    __IDENTITY integer;");
            }

            sb.Append($@"
BEGIN
    INSERT INTO {fullTableName} ({GetColumnNameList(dbName, schema, dataset, propData, objType, Add: true, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");

            sb.RemoveLastComma();
            sb.Append($@")
    VALUES({GetValueNameList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");

            sb.RemoveLastComma();
            sb.Append($@")");

            if (HasIdentity(identityName)) {
                sb.Append($@"
    RETURNING ""{identityName}"" INTO __IDENTITY");
            }
            sb.Append($@"
;");

            foreach (SubTableInfo subTable in subTables) {
                List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                sb.Add($@"
    __COUNT = ARRAY_LENGTH(""arg{subTable.PropInfo.Name}"", 1);
    IF __COUNT IS NOT NULL THEN
        FOR ctr IN 1..__COUNT LOOP
            INSERT INTO {sb.BuildFullTableName(dbName, schema, subTable.Name)} ({GetColumnNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Add: true, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");

                sb.RemoveLastComma();
                sb.Append($@")
            VALUES({GetValueNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Prefix: $@"""arg{subTable.PropInfo.Name}""[ctr]", TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");
                sb.RemoveLastComma();
                sb.Append($@");
        END LOOP;
    END IF
;");
            }

            if (HasIdentity(identityName)) {
                sb.Append($@"
    RETURN (SELECT __IDENTITY); --- result set");
            }

            sb.Append($@"
END;
$$;");

            // UPDATE
            // UPDATE
            // UPDATE

            sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Update"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__Update""(""Key1Val"" {typeKey1},");
            if (!string.IsNullOrWhiteSpace(key2Name))
                sb.Append($@"""Key2Val"" {typeKey2},");
            sb.Append($@"{GetArgumentNameList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");

            sb.RemoveLastComma();

            sb.Append($@")
    RETURNS integer
    LANGUAGE 'plpgsql'
AS $$
DECLARE
    __TOTAL integer;
    __COUNT integer;");

            if (HasIdentity(identityName)) {
                sb.Append($@"
    __IDENTITY integer;");
            }

            sb.Append($@"
BEGIN
    UPDATE {fullTableName}
    SET {GetSetList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");

            sb.RemoveLastComma();

            sb.Append($@"
    WHERE ""{key1Name}"" = ""Key1Val""");
        if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND ""{key2Name}"" = ""Key2Val""");
        if (siteIdentity > 0) sb.Append($@" AND ""{SQLGenericBase.SiteColumn}"" = ""{SQLGen.ValSiteIdentity}""");

            if (HasIdentity(identityName)) {
                sb.Append($@"
    RETURNING ""{identityName}"" INTO __IDENTITY");
            }
            sb.Append($@"
;
    GET DIAGNOSTICS __TOTAL = ROW_COUNT
;");

            foreach (SubTableInfo subTable in subTables) {

                List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                sb.Add($@"
    DELETE FROM {sb.BuildFullTableName(dbName, schema, subTable.Name)}
        WHERE ""{SQLGenericBase.SubTableKeyColumn}"" = __IDENTITY ;

    __COUNT = ARRAY_LENGTH(""arg{subTable.PropInfo.Name}"", 1);
    IF __COUNT IS NOT NULL THEN
        FOR ctr IN 1..__COUNT LOOP
            INSERT INTO {sb.BuildFullTableName(dbName, schema, subTable.Name)} ({GetColumnNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Add: true, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");

                sb.RemoveLastComma();
                sb.Append($@")
            VALUES({GetValueNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Prefix: $@"""arg{subTable.PropInfo.Name}""[ctr]", TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");
                sb.RemoveLastComma();
                sb.Append($@");
        END LOOP;
    END IF
;");
            }

            sb.Append($@"

    RETURN (SELECT __TOTAL); --- result set
END;
$$;");


            // UPDATE BY IDENTITY
            // UPDATE BY IDENTITY
            // UPDATE BY IDENTITY

            if (HasIdentity(identityName)) {

                sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__UpdateByIdentity"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__UpdateByIdentity""({GetArgumentNameList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: false, WithDerivedInfo: false, SubTable: false)}""valIdentity"" integer,");

                sb.RemoveLastComma();

                sb.Append($@")
    RETURNS integer
    LANGUAGE 'plpgsql'
AS $$
DECLARE
    __TOTAL integer;
    __COUNT integer;
    __IDENTITY integer;");

                sb.Append($@"
BEGIN
    __IDENTITY = ""valIdentity"";

    UPDATE {fullTableName}
    SET {GetSetList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: false, WithDerivedInfo: false, SubTable: false)}");

                sb.RemoveLastComma();

                sb.Append($@"
    WHERE ""{identityName}"" = __IDENTITY
;
    GET DIAGNOSTICS __TOTAL = ROW_COUNT
;");

                foreach (SubTableInfo subTable in subTables) {

                    List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                    sb.Add($@"
    DELETE FROM {sb.BuildFullTableName(dbName, schema, subTable.Name)}
        WHERE ""{SQLGenericBase.SubTableKeyColumn}"" = __IDENTITY;

    __COUNT = ARRAY_LENGTH(""arg{subTable.PropInfo.Name}"", 1);
    IF __COUNT IS NOT NULL THEN
        FOR ctr IN 1..__COUNT LOOP
            INSERT INTO {sb.BuildFullTableName(dbName, schema, subTable.Name)} ({GetColumnNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Add: true, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");

                    sb.RemoveLastComma();
                    sb.Append($@")
            VALUES({GetValueNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Prefix: $@"""arg{subTable.PropInfo.Name}""[ctr]", TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");
                    sb.RemoveLastComma();
                    sb.Append($@");
        END LOOP;
    END IF
;");
                }

                sb.Append($@"

    RETURN (SELECT __TOTAL); --- result set
END;
$$;");
            }

            // REMOVE
            // REMOVE
            // REMOVE

            sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Remove"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__Remove""(""Key1Val"" {typeKey1},");
            if (!string.IsNullOrWhiteSpace(key2Name))
                sb.Append($@"""Key2Val"" {typeKey2},");
            if (siteIdentity > 0)
                sb.Append($@"""{SQLGen.ValSiteIdentity}"" integer,");

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
    WHERE ""{key1Name}"" = ""Key1Val""");
                if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND ""{key2Name}"" = ""Key2Val""");
                if (siteIdentity > 0) sb.Append($@" AND ""{SQLGenericBase.SiteColumn}"" = ""{SQLGen.ValSiteIdentity}""");

                sb.Append($@"
;");

            } else {

                sb.Append($@"
    DECLARE __IDENTITY integer;
BEGIN
    SELECT ""{GetIdentityNameOrDefault(identityName)}"" INTO __IDENTITY FROM {fullTableName}
    WHERE ""{key1Name}"" = ""Key1Val""");
    if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND ""{key2Name}"" = ""Key2Val""");
    if (siteIdentity > 0) sb.Append($@" AND ""{SQLGenericBase.SiteColumn}"" = ""{SQLGen.ValSiteIdentity}""");
                sb.Append($@"
;");

                foreach (SubTableInfo subTable in subTables) {
                    sb.Add($@"
    DELETE FROM {sb.BuildFullTableName(dbName, schema, subTable.Name)} WHERE {sb.BuildFullColumnName(subTable.Name, SQLGenericBase.SubTableKeyColumn)} = __IDENTITY
;");
                }
                sb.Add($@"
    DELETE FROM {fullTableName} WHERE ""{GetIdentityNameOrDefault(identityName)}"" = __IDENTITY
;");
                
            }

            sb.Append($@"
    GET DIAGNOSTICS __TOTAL = ROW_COUNT
;
    RETURN (SELECT __TOTAL); --- result set
END;
$$;");

            // REMOVE BY IDENTITY
            // REMOVE BY IDENTITY
            // REMOVE BY IDENTITY

            if (HasIdentity(identityName)) {

                sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__RemoveByIdentity"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__RemoveByIdentity""(""valIdentity"" integer)
    RETURNS integer
    LANGUAGE 'plpgsql'
AS $$
    DECLARE __TOTAL integer;
    DECLARE __IDENTITY integer;
BEGIN
    __IDENTITY = ""valIdentity"";");

                foreach (SubTableInfo subTable in subTables) {
                    sb.Add($@"
    DELETE FROM {sb.BuildFullTableName(dbName, schema, subTable.Name)} WHERE {sb.BuildFullColumnName(subTable.Name, SQLGenericBase.SubTableKeyColumn)} = __IDENTITY
;");
                }
                sb.Add($@"
    DELETE FROM {fullTableName} WHERE ""{identityName}"" = __IDENTITY
;
    GET DIAGNOSTICS __TOTAL = ROW_COUNT
;
    RETURN (SELECT __TOTAL); --- result set
END;
$$;");
            }

            // Add to database
            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = Conn;
                cmd.CommandText = sb.ToString();
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }

            return true;
        }

        internal Task<bool> DropFunctionsAsync(string dbName, string schema, string dataset) {

            SQLBuilder sb = new SQLBuilder();

            sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Get"";
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__GetByIdentity"";
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Add"";
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Update"";
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__UpdateByIdentity"";
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Remove"";
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__RemoveByIdentity"";
");

            using (NpgsqlCommand cmd = new NpgsqlCommand()) {
                cmd.Connection = Conn;
                cmd.CommandText = sb.ToString();
                YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                cmd.ExecuteNonQuery();
            }
            return Task.FromResult(true);
        }

        internal static async Task<string> CalculatedPropertiesAsync(Type objType, Func<string, Task<string>> calculatedPropertyCallbackAsync) {
            if (calculatedPropertyCallbackAsync == null) return null;
            SQLBuilder sb = new SQLBuilder();
            List<PropertyData> props = ObjectSupport.GetPropertyData(objType);
            props = (from p in props where p.CalculatedProperty select p).ToList();
            foreach (PropertyData prop in props) {
                string calcProp = await calculatedPropertyCallbackAsync(prop.Name);
                sb.Add($@"({calcProp}) AS ""{prop.Name}"",");
            }
            return sb.ToString();
        }

        protected string GetIdentityNameOrDefault(string identityName) {
            if (string.IsNullOrWhiteSpace(identityName))
                identityName = SQLGenericBase.IdentityColumn;
            return identityName;
        }

        internal string GetArgumentNameList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, string Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            SQLManager sqlManager = new SQLManager();
            return ProcessColumns(
                (prefix, container, prop) => { // prop
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    string colType = GetDataTypeArgumentString(col);
                    return $@"""arg{prefix}{prop.Name}"" {colType},";
                },
                (prefix, container, prop) => { 
                    return null; 
                }, // Identity
                (prefix, container, prop) => { // binary
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    string colType = GetDataTypeArgumentString(col);
                    return $@"""arg{prefix}{prop.Name}"" {colType},";
                },
                (prefix, container, prop) => { // image
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    string colType = GetDataTypeArgumentString(col);
                    return $@"""arg{prefix}{prop.Name}"" {colType},";
                },
                (prefix, container, prop) => { // multistring
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sb = new StringBuilder();
                    foreach (LanguageData lang in Languages) {
                        string colName = $"{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, $"{prefix}{prop.Name}")}";
                        Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, colName);
                        string colType = GetDataTypeArgumentString(col);
                        sb.Append($@"""arg{colName}"" {colType},");
                    }
                    return sb.ToString();
                },
                (prefix, container, name) => { // predef
                    if (name == SQLGenericBase.SubTableKeyColumn)
                        return null;
                    if (name == "DerivedTableName")
                        return $@"""valDerivedTableName"" character varying,";
                    if (name == "DerivedDataType") 
                        return $@"""valDerivedDataType"" character varying,";
                    if (name == "DerivedAssemblyName") 
                        return $@"""valDerivedAssemblyName"" character varying,";
                    string colType = "character varying";
                    if (name == SQLGenericBase.SiteColumn) {
                        name = SQLGen.ValSiteIdentity;
                        colType = "integer";
                    }
                    return $@"""{prefix}{name}"" {colType},";
                },
                (prefix, container, prop, subPropData, subType, subtableName) => { // Subtable
                    if (subPropData.Count == 1) {
                        Column col = sqlManager.GetColumn(Conn, dbName, schema, subtableName, $"{prefix}{subPropData[0].ColumnName}");
                        string colType = GetDataTypeArgumentString(col);
                        return $@"""arg{prefix}{prop.ColumnName}"" {colType}[],";
                    } else {
                        return $@"""arg{prefix}{prop.ColumnName}"" ""{subtableName}_T""[],";
                    }
                },
                dbName, schema, dataset, null, propData, tpContainer, new List<string>(), Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal string GetTypeNameList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, string Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            SQLManager sqlManager = new SQLManager();
            return ProcessColumns(
                (prefix, container, prop) => {
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    string colType = GetDataTypeArgumentString(col);
                    return $@"""{prefix}{prop.Name}"" {colType},";
                },
                (prefix, container, prop) => { 
                    if (!SubTable) {
                        Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                        string colType = GetDataTypeArgumentString(col);
                        return $@"""{prefix}{prop.Name}"" {colType},";
                    }
                    return null; 
                }, // Identity
                (prefix, container, prop) => {
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    string colType = GetDataTypeArgumentString(col);
                    return $@"""{prefix}{prop.Name}"" {colType},";
                },
                (prefix, container, prop) => {
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    string colType = GetDataTypeArgumentString(col);
                    return $@"""{prefix}{prop.Name}"" {colType},";
                },
                (prefix, container, prop) => {
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sb = new StringBuilder();
                    foreach (LanguageData lang in Languages) {
                        string colName = $"{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, $"{prefix}{prop.Name}")}";
                        Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, colName);
                        string colType = GetDataTypeArgumentString(col);
                        sb.Append($@"""{colName}"" {colType},");
                    }
                    return sb.ToString();
                },
                (prefix, container, name) => { // predef
                    if (name == SQLGenericBase.SubTableKeyColumn)
                        return null;
                    string colType = "character varying";
                    if (name == SQLGenericBase.SiteColumn) {
                        name = SQLGen.ValSiteIdentity;
                        colType = "integer";
                    }
                    return $@"""{prefix}{name}"" {colType},";
                },
                (prefix, container, prop, subPropData, subType, subtableName) => { // Subtable
                    if (subPropData.Count == 1) {
                        Column col = sqlManager.GetColumn(Conn, dbName, schema, subtableName, $"{prefix}{subPropData[0].ColumnName}");
                        string colType = GetDataTypeArgumentString(col);
                        return $@"""{prefix}{prop.ColumnName}"" {colType}[],";
                    }
                    return $@"""{prefix}{prop.ColumnName}"" ""{subtableName}_T""[],";
                },
                dbName, schema, dataset, null, propData, tpContainer, new List<string>(), Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal string GetColumnNameList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, bool Add = false, string Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false,
                Dictionary<string, string> VisibleColumns = null) {
            SQLBuilder sb = new SQLBuilder();
            return ProcessColumns(
                (prefix, container, prop) => {
                    string col = $@"{prefix}{prop.ColumnName}";
                    if (VisibleColumns != null) {
                        if (VisibleColumns.ContainsKey(col)) return null;
                        string fullCol = sb.BuildFullColumnName(dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                        VisibleColumns.Add($"{prefix}{prop.ColumnName}", fullCol);
                        return $"{fullCol},";
                    }
                    return $@"""{prefix}{prop.ColumnName}"","; 
                },
                (prefix, container, prop) => { // Identity
                    if (Add) {
                        return null;
                    } else {
                        string col = $@"{prefix}{prop.ColumnName}";
                        if (VisibleColumns != null) {
                            if (VisibleColumns.ContainsKey(col)) return null;
                            string fullCol = sb.BuildFullColumnName(dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                            VisibleColumns.Add($"{prefix}{prop.ColumnName}", fullCol);
                            return $"{fullCol},";
                        }
                        return $@"""{prefix}{prop.ColumnName}"",";
                    }
                },
                (prefix, container, prop) => {
                    string col = $@"{prefix}{prop.ColumnName}";
                    if (VisibleColumns != null) {
                        if (VisibleColumns.ContainsKey(col)) return null;
                        string fullCol = sb.BuildFullColumnName(dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                        VisibleColumns.Add($"{prefix}{prop.ColumnName}", fullCol);
                        return $"{fullCol},";
                    }
                    return $@"""{prefix}{prop.ColumnName}"",";
                },
                (prefix, container, prop) => {
                    string col = $@"{prefix}{prop.ColumnName}";
                    if (VisibleColumns != null) {
                        if (VisibleColumns.ContainsKey(col)) return null;
                        string fullCol = sb.BuildFullColumnName(dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                        VisibleColumns.Add($"{prefix}{prop.ColumnName}", fullCol);
                        return $"{fullCol},";
                    }
                    return $@"""{prefix}{prop.ColumnName}"",";
                },
                (prefix, container, prop) => {
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sbldr = new StringBuilder();
                    foreach (LanguageData lang in Languages) {
                        string col = $@"{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name)}";
                        if (VisibleColumns != null) {
                            if (VisibleColumns.ContainsKey(col)) return null;
                            string fullCol = sb.BuildFullColumnName(dbName, schema, dataset, $"{prefix}{col}");
                            VisibleColumns.Add(col, fullCol);
                            sbldr.Append($"{fullCol},");
                        } else
                            sbldr.Append($@"""{prefix}{col}"",");
                    }
                    return sbldr.ToString();
                },
                (prefix, container, name) => { // predef
                    if (Add) {
                        string col = $@"{prefix}{name}";
                        if (VisibleColumns != null) {
                            if (VisibleColumns.ContainsKey(col)) return null;
                            string fullCol = sb.BuildFullColumnName(dbName, schema, dataset, $"{prefix}{name}");
                            VisibleColumns.Add($"{prefix}{name}", fullCol);
                            return $"{fullCol},";
                        }
                        return $@"""{prefix}{name}"",";
                    } else {
                        return null;
                    }
                },
                (prefix, container, prop, subPropData, subType, subtableName) => { // Subtable
                    return null;
                },
                dbName, schema, dataset, null, propData, tpContainer, new List<string>(), Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal string GetValueNameList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, string Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            return ProcessColumns(
                (prefix, container, prop) => {
                    if (SubTable) {
                        if (propData.Count == 1)
                            return $"{Prefix},";
                        return $@"{Prefix}.""{prop.Name}"",";
                    } else
                        return $@"""arg{prefix}{prop.Name}"",";
                },
                (prefix, container, prop) => { return null; }, // Identity
                (prefix, container, prop) => { // Binary
                    if (SubTable) {
                        if (propData.Count == 1)
                            return $"{Prefix},";
                        return $@"{Prefix}.""{prop.Name}"",";
                    } else
                        return $@"""arg{prefix}{prop.Name}"",";
                },
                (prefix, container, prop) => { // Image
                    if (SubTable) {
                        if (propData.Count == 1)
                            return $"{Prefix},";
                        return $@"{Prefix}.""{prop.Name}"",";
                    } else
                        return $@"""arg{prefix}{prop.Name}"",";
                },
                (prefix, container, prop) => { // Language
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sb = new StringBuilder();
                    foreach (LanguageData lang in Languages) {
                        sb.Append($@"""arg{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name)}"",");
                    }
                    return sb.ToString();
                },
                (prefix, container, name) => { // predef
                    if (name == "DerivedTableName")
                        return $@"""valDerivedTableName"",";
                    if (name == "DerivedDataType")
                        return $@"""valDerivedDataType"",";
                    if (name == "DerivedAssemblyName")
                        return $@"""valDerivedAssemblyName"",";
                    if (name == SQLGenericBase.SiteColumn)
                        return $@"""{SQLGen.ValSiteIdentity}"",";
                    if (name == SQLGenericBase.SubTableKeyColumn)
                        return $"__IDENTITY,";
                    return $@"""arg{prefix}{name}"","; 
                },
                (prefix, container, prop, subPropData, subType, subtableName) => { // Subtable
                    return null;
                },
                dbName, schema, dataset, null, propData, tpContainer, new List<string>(), Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal string GetSetList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, bool Add = false, string Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            if (SubTable) throw new InternalError($"{nameof(GetSetList)} called for subtable which is not supported");
            return ProcessColumns(
                (prefix, container, prop) => { return $@"""{prefix}{prop.ColumnName}""=""arg{prefix}{prop.Name}"","; },
                (prefix, container, prop) => { return null; }, // Identity
                (prefix, container, prop) => { return $@"""{prefix}{prop.ColumnName}""=""arg{prefix}{prop.Name}"","; },
                (prefix, container, prop) => { return $@"""{prefix}{prop.ColumnName}""=""arg{prefix}{prop.Name}"","; },
                (prefix, container, prop) => {
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sb = new StringBuilder();
                    foreach (LanguageData lang in Languages)
                        sb.Append($@"""{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name)}""=""arg{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name)}"",");
                    return sb.ToString();
                },
                (prefix, container, name) => {
                    return null;
                },
                (prefix, container, prop, subPropData, subType, subtableName) => {
                    return null;
                },
                dbName, schema, dataset, null, propData, tpContainer, new List<string>(), Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal static string ProcessColumns(
                Func<string, object, PropertyData, string> fmt,
                Func<string, object, PropertyData, string> fmtIdentity,
                Func<string, object, PropertyData, string> fmtBinary,
                Func<string, object, PropertyData, string> fmtImage,
                Func<string, object, PropertyData, string> fmtLanguage, 
                Func<string, object, string, string> fmtPredef, 
                Func<string, object, PropertyData, List<PropertyData>, Type, string, string> fmtSubtable,
                string dbName, string schema, string dataset, 
                object container, List<PropertyData> propData, Type tpContainer, List<string> columns,
                string Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {

            SQLBuilder sb = new SQLBuilder();
            foreach (PropertyData prop in propData) {
                Type propertyType = prop.PropInfo.PropertyType;
                if (prop.PropInfo.CanRead && prop.PropInfo.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    string colName = prop.ColumnName;
                    if (!columns.Contains(colName)) {
                        columns.Add(colName);
                        if (prop.HasAttribute(Data_Identity.AttributeName)) {
                            if (SubTable)
                                throw new InternalError("Subtables can't have an explicit identity");
                            if (propertyType != typeof(int))
                                throw new InternalError("Identity columns must be of type int");
                            sb.Add(fmtIdentity(Prefix, container, prop));
                        } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                            sb.Add(fmtBinary(Prefix, container, prop));
                        } else if (propertyType == typeof(MultiString)) {
                            sb.Add(fmtLanguage(Prefix, container, prop));
                        } else if (propertyType == typeof(Image)) {
                            sb.Add(fmtImage(Prefix, container, prop));
                        } else if (SQLGenericBase.TryGetDataType(propertyType)) {
                            sb.Add(fmt(Prefix, container, prop));
                        } else if (propertyType.IsClass && typeof(IEnumerable).IsAssignableFrom(propertyType)) {
                            // This is a enumerated type, so we have to create separate values using this table's identity column as a link
                            // these values are added as a subtable
                            if (SubTable) throw new InternalError("Nested subtables not supported");
                            PropertyInfo pi = prop.PropInfo;
                            Type subType = pi.PropertyType.GetInterfaces().Where(t => t.IsGenericType == true && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                                    .Select(t => t.GetGenericArguments()[0]).FirstOrDefault();
                            List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subType);
                            string subTableName = dataset + "_" + prop.Name;
                            sb.Add(fmtSubtable(Prefix, container, prop, subPropData, subType, subTableName));
                        } else if (propertyType.IsClass) {
                            object sub = null;
                            if (container != null)
                                sub = prop.PropInfo.GetValue(container);
                            List<PropertyData> subPropData = ObjectSupport.GetPropertyData(propertyType);
                            string args = ProcessColumns(fmt, fmtIdentity, fmtBinary, fmtImage, fmtLanguage, fmtPredef, fmtSubtable, dbName, schema, dataset, sub, subPropData, propertyType, columns, Prefix + prop.Name + "_", TopMost, SiteSpecific: false, WithDerivedInfo: false, SubTable: false);
                            sb.Add(args);
                        } else
                            throw new InternalError($"Unknown property type {propertyType.FullName} used in class {tpContainer.FullName}, property {colName}");
                    }
                }
            }
            if (SiteSpecific)
                sb.Add(fmtPredef(Prefix, container, SQLGenericBase.SiteColumn));
            if (WithDerivedInfo) {
                sb.Add(fmtPredef(Prefix, container, "DerivedTableName"));
                sb.Add(fmtPredef(Prefix, container, "DerivedDataType"));
                sb.Add(fmtPredef(Prefix, container, "DerivedAssemblyName"));
            }
            if (SubTable)
                sb.Add(fmtPredef(Prefix, container, SQLGenericBase.SubTableKeyColumn));
            return sb.ToString();
        }
    }
}