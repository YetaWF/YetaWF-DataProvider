/* Copyright © 2022 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using YetaWF.Core.Models;
using YetaWF.DataProvider.SQLGeneric;

namespace YetaWF.DataProvider.PostgreSQL {

    internal partial class SQLGen {

        internal const string ValDerivedTableName = "valDerivedTableName"; // sproc argument for derived table name
        internal const string ValDerivedDataType = "valDerivedDataType"; // sproc argument for derived type
        internal const string ValDerivedAssemblyName = "valDerivedAssemblyName"; // sproc argument for derived assembly name

        internal const string DerivedTableName = "DerivedTableName"; // column names
        internal const string DerivedDataType = "DerivedDataType";
        internal const string DerivedAssemblyName = "DerivedAssemblyName";

        internal bool MakeFunctionsWithBaseTypeAsync(string dbName, string schema, string baseDataset, string dataset, string key1Name, string identityName, List<PropertyData> combinedProps, List<PropertyData> basePropData, List<PropertyData> propData, Type baseType, Type type, int siteIdentity) {

            SQLManager sqlManager = new SQLManager();
            SQLBuilder sb = new SQLBuilder();
            SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

            string fullTableName = sb.GetTable(dbName, schema, dataset);
            string fullBaseTableName = sb.GetTable(dbName, schema, baseDataset);

            Column colKey1 = sqlManager.GetColumn(Conn, dbName, schema, dataset, key1Name);
            string typeKey1 = GetDataTypeArgumentString(colKey1);

            List<PropertyData> propDataNoDups = combinedProps.Except(basePropData, new PropertyDataComparer()).ToList();

            // GET
            // GET
            // GET

            sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Get"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__Get""(""Key1Val"" {typeKey1}, ""{SQLGen.ValSiteIdentity}"" integer,");

            sb.RemoveLastComma();
            sb.Append($@")");

            Dictionary<string, string> visibleColumns = new Dictionary<string, string>();

            sb.Append($@"
RETURNS SETOF ""{schema}"".""{dataset}_T""
LANGUAGE 'plpgsql'
AS $$
BEGIN
    RETURN QUERY (
        SELECT {GetColumnNameList(dbName, schema, baseDataset, basePropData, baseType, Prefix: null, TopMost: false, SiteSpecific: true, WithDerivedInfo: false, SubTable: false, VisibleColumns: visibleColumns)}{GetColumnNameList(dbName, schema, dataset, propDataNoDups, type, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: false, VisibleColumns: visibleColumns)}");

            sb.RemoveLastComma();

            sb.Append($@"
        FROM {fullBaseTableName}
        LEFT JOIN {fullTableName} ON {fullBaseTableName}.""{key1Name}"" = {fullTableName}.""{key1Name}"" AND {fullBaseTableName}.""{SQLGenericBase.SiteColumn}"" = {fullTableName}.""{SQLGenericBase.SiteColumn}""
        WHERE {fullBaseTableName}.""{key1Name}"" = ""Key1Val"" AND {fullBaseTableName}.""{SQLGenericBase.SiteColumn}"" = ""{SQLGen.ValSiteIdentity}""
        LIMIT 1    --- result set
    )
;
END;
$$;");

            // ADD
            // ADD
            // ADD

            sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Add"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__Add""({GetArgumentNameList(dbName, schema, baseDataset, basePropData, baseType, Prefix: null, TopMost: false, SiteSpecific: true, WithDerivedInfo: true, SubTable: false)}{GetArgumentNameList(dbName, schema, dataset, propDataNoDups, type, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: false)}");

            sb.RemoveLastComma();
            sb.Append($@")
RETURNS integer
LANGUAGE 'plpgsql'
AS $$
DECLARE
    __TOTAL integer;
BEGIN
    INSERT INTO {fullBaseTableName} ({GetColumnNameList(dbName, schema, baseDataset, basePropData, baseType, Add: true, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: true, SubTable: false)}");

            sb.RemoveLastComma();
            sb.Append($@")
    VALUES({GetValueNameList(dbName, schema, baseDataset, basePropData, baseType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: true, SubTable: false)}");

            sb.RemoveLastComma();
            sb.Append($@")
;
    GET DIAGNOSTICS __TOTAL = ROW_COUNT
;");

            sb.Append($@"
    INSERT INTO {fullTableName} ({GetColumnNameList(dbName, schema, dataset, propData, type, Add: true, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");

            sb.RemoveLastComma();
            sb.Append($@")
    VALUES({GetValueNameList(dbName, schema, dataset, propData, type, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");

            sb.RemoveLastComma();
            sb.Append($@")
;
    RETURN (SELECT __TOTAL); --- result set
END;
            $$;");

            // UPDATE
            // UPDATE
            // UPDATE

            sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Update"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__Update""({GetArgumentNameList(dbName, schema, baseDataset, basePropData, baseType, Prefix: null, TopMost: false, SiteSpecific: true, WithDerivedInfo: true, SubTable: false)}{GetArgumentNameList(dbName, schema, dataset, propDataNoDups, type, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: false)}");

            sb.RemoveLastComma();

            sb.Append($@")
RETURNS integer
LANGUAGE 'plpgsql'
AS $$
DECLARE
    __TOTAL integer;");

            basePropData = (from p in basePropData where p.Name != key1Name select p).ToList();// remove ModuleGuid from set list, module guid doesn't change
            propData = (from p in propData where p.Name != key1Name select p).ToList();// remove ModuleGuid from set list

            sb.Append($@"
BEGIN
    UPDATE {fullBaseTableName}
    SET {GetSetList(dbName, schema, baseDataset, basePropData, baseType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");

            sb.RemoveLastComma();

            sb.Append($@"
    WHERE ""{key1Name}"" = ""arg{key1Name}"" AND ""{SQLGenericBase.SiteColumn}"" = ""{SQLGen.ValSiteIdentity}""
;
    GET DIAGNOSTICS __TOTAL = ROW_COUNT
;");

            string setList = GetSetList(dbName, schema, dataset, propData, type, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false);
            if (!string.IsNullOrWhiteSpace(setList)) {

                sb.Append($@"
    UPDATE {fullTableName}
    SET {setList}");

                sb.RemoveLastComma();

                sb.Append($@"
    WHERE ""{key1Name}"" = ""arg{key1Name}"" AND ""{SQLGenericBase.SiteColumn}"" = ""{SQLGen.ValSiteIdentity}""
;");
            }

            sb.Append($@"
    RETURN (SELECT __TOTAL); --- result set
END;
$$;");


            // REMOVE
            // REMOVE
            // REMOVE

            sb.Append($@"
DROP FUNCTION IF EXISTS ""{schema}"".""{dataset}__Remove"";
CREATE OR REPLACE FUNCTION ""{schema}"".""{dataset}__Remove""(""Key1Val"" {typeKey1}, ""valSiteIdentity"" integer)
RETURNS integer
LANGUAGE 'plpgsql'
AS $$
DECLARE
    __TOTAL integer;");

            sb.Append($@"
BEGIN
    DELETE FROM {fullBaseTableName}
    WHERE ""{key1Name}"" = ""Key1Val"" AND ""{SQLGenericBase.SiteColumn}"" = ""{SQLGen.ValSiteIdentity}""
;
    GET DIAGNOSTICS __TOTAL = ROW_COUNT
;

    DELETE FROM {fullTableName}
    WHERE ""{key1Name}"" = ""Key1Val"" AND ""{SQLGenericBase.SiteColumn}"" = ""{SQLGen.ValSiteIdentity}""
;

    RETURN (SELECT __TOTAL); --- result set
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
   }
}