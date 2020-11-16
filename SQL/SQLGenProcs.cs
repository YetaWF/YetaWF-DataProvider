/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using Microsoft.Data.SqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Language;
using YetaWF.Core.Models;
using YetaWF.Core.Support;
using YetaWF.DataProvider.SQLGeneric;

namespace YetaWF.DataProvider.SQL {

    internal partial class SQLGen {

        public const string ValSiteIdentity = "valSiteIdentity"; // sproc argument with site identity

        internal async Task<bool> MakeFunctionsAsync(string dbName, string schema, string dataset, string key1Name, string? key2Name, string identityName, List<PropertyData> propData, Type objType, int siteIdentity,
                Func<string, Task<string>>? calculatedPropertyCallbackAsync) {

            using (new SQLBuilder.GeneratingProcs()) {

                SQLManager sqlManager = new SQLManager();
                SQLBuilder sb = new SQLBuilder();
                SQLHelper sqlHelper = new SQLHelper(Conn, null, Languages);

                string fullTableName = sb.GetTable(dbName, schema, dataset);
                List<SubTableInfo> subTables = GetSubTables(dataset, propData);

                Column colKey1 = sqlManager.GetColumn(Conn, dbName, schema, dataset, key1Name);
                string typeKey1 = GetDataTypeArgumentString(colKey1);

                Column? colKey2 = null;
                string? typeKey2 = null;
                if (!string.IsNullOrWhiteSpace(key2Name)) {
                    colKey2 = sqlManager.GetColumn(Conn, dbName, schema, dataset, key2Name);
                    typeKey2 = GetDataTypeArgumentString(colKey2);
                }

                // GET
                // GET
                // GET

                sb.Append($@"
IF EXISTS (
    SELECT sys.procedures.name FROM sys.procedures WITH(NOLOCK) 
    WHERE type = 'P' AND schema_id = SCHEMA_ID('{schema}') AND name = '{dataset}__Get' 
) DROP PROCEDURE [{schema}].[{dataset}__Get]

GO

CREATE PROCEDURE [{schema}].[{dataset}__Get]
(
    @Key1Val {typeKey1},");
                if (!string.IsNullOrWhiteSpace(key2Name))
                    sb.Append($@"
    @Key2Val {typeKey2},");
                if (siteIdentity > 0)
                    sb.Append($@"
    @{SQLGen.ValSiteIdentity} integer,");

                sb.RemoveLastComma();
                sb.Append($@"
)
AS
BEGIN
    SELECT TOP 1 {GetColumnNameList(dbName, schema, dataset, propData, objType, Add: false, Prefix: null, TopMost: false, IdentityName: identityName, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");
                if (calculatedPropertyCallbackAsync != null) sb.Append(await CalculatedPropertiesAsync(objType, calculatedPropertyCallbackAsync));

                sb.RemoveLastComma();

                sb.Append($@"
    FROM {fullTableName}
    WHERE [{key1Name}] = @Key1Val");
                if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND [{key2Name}] = @Key2Val");
                if (siteIdentity > 0) sb.Append($@" AND [{SQLGenericBase.SiteColumn}] = @{SQLGen.ValSiteIdentity}");

                sb.Append($@"
; --- result set
");

                foreach (SubTableInfo subTable in subTables) {
                    List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                    sb.Add($@"
    SELECT {GetColumnNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Add: false, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");
                    sb.RemoveLastComma();
                    sb.Add($@"
    FROM {sb.BuildFullTableName(dbName, schema, subTable.Name)}
    INNER JOIN {sb.BuildFullTableName(dbName, schema, dataset)} ON {sb.BuildFullColumnName(dataset, GetIdentityNameOrDefault(identityName))} = {sb.BuildFullColumnName(subTable.Name, SQLGenericBase.SubTableKeyColumn)}
    WHERE {sb.BuildFullColumnName(dataset, key1Name)} = @Key1Val");
                    if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND {sb.BuildFullColumnName(dataset, key2Name)} = @Key2Val");
                    if (siteIdentity > 0) sb.Append($@" AND {sb.BuildFullColumnName(dataset, SQLGenericBase.SiteColumn)} = @{SQLGen.ValSiteIdentity}");

                    sb.Append($@"
;  --- result set
");
                }

                sb.Append($@"
END

GO

");

                // GET BY IDENTITY
                // GET BY IDENTITY
                // GET BY IDENTITY

                if (HasIdentity(identityName)) {

                    sb.Append($@"
IF EXISTS (
    SELECT sys.procedures.name FROM sys.procedures WITH(NOLOCK) 
    WHERE type = 'P' AND schema_id = SCHEMA_ID('{schema}') AND name = '{dataset}__GetByIdentity' 
) DROP PROCEDURE [{schema}].[{dataset}__GetByIdentity]

GO

CREATE PROCEDURE [{schema}].[{dataset}__GetByIdentity]
(
    @ValIdentity integer,");
                    sb.RemoveLastComma();
                    sb.Append($@"
)
AS
BEGIN
    SELECT TOP 1 {GetColumnNameList(dbName, schema, dataset, propData, objType, Add: false, Prefix: null, TopMost: false, IdentityName: identityName, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");
                    if (calculatedPropertyCallbackAsync != null) sb.Append(await CalculatedPropertiesAsync(objType, calculatedPropertyCallbackAsync));

                    sb.RemoveLastComma();

                    sb.Append($@"
    FROM {fullTableName}
    WHERE [{identityName}] = @ValIdentity
; --- result set
");

                    foreach (SubTableInfo subTable in subTables) {
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                        sb.Add($@"
    SELECT {GetColumnNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Add: false, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");
                        sb.RemoveLastComma();
                        sb.Add($@"
    FROM {sb.BuildFullTableName(dbName, schema, subTable.Name)}
    WHERE {sb.BuildFullColumnName(subTable.Name, SQLGenericBase.SubTableKeyColumn)} = @ValIdentity
;  --- result set
");
                    }

                    sb.Append($@"
END

GO

");

                }

                // ADD
                // ADD
                // ADD

                sb.Append($@"
IF EXISTS (
    SELECT sys.procedures.name FROM sys.procedures WITH(NOLOCK) 
    WHERE type = 'P' AND schema_id = SCHEMA_ID('{schema}') AND name = '{dataset}__Add' 
) DROP PROCEDURE [{schema}].[{dataset}__Add]

GO

CREATE PROCEDURE [{schema}].[{dataset}__Add]
(
    {GetArgumentNameList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");
                sb.RemoveLastComma();
                sb.Append($@"
)
AS
BEGIN
    INSERT INTO {fullTableName} ({GetColumnNameList(dbName, schema, dataset, propData, objType, Add: true, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");

                sb.RemoveLastComma();
                sb.Append($@")
    VALUES({GetValueNameList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");

                sb.RemoveLastComma();
                sb.Append($@")
");

                if (HasIdentity(identityName) || subTables.Count > 0) {
                    sb.Append($@"
    DECLARE @__IDENTITY int = @@IDENTITY");

                    if (subTables.Count > 0) {
                        sb.Append($@"
    IF @__IDENTITY IS NOT NULL
    BEGIN
");
                    }
                } else {
                    sb.Append($@"
    DECLARE @__ROWCOUNT int = @@ROWCOUNT;");

                    if (subTables.Count > 0) {
                        sb.Append($@"
    IF @__ROWCOUNT > 0
    BEGIN");
                    }
                }

                foreach (SubTableInfo subTable in subTables) {
                    List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                    sb.Add($@"

        INSERT INTO {sb.BuildFullTableName(dbName, schema, subTable.Name)} ({GetColumnNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Add: true, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");


                    sb.RemoveLastComma();
                    sb.Append($@")
        SELECT {GetValueNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");
                    sb.RemoveLastComma();
                    sb.Append($@"
        FROM @arg{subTable.PropInfo.Name}
");
                }

                if (subTables.Count > 0) {
                    sb.Append($@"
    END");
                }

                if (HasIdentity(identityName) || subTables.Count > 0) {
                    sb.Append($@"
    SELECT @__IDENTITY  --- result set");
                } else {
                    sb.Append($@"
    SELECT @__ROWCOUNT  --- result set");
                }

                sb.Append($@"
END

GO

");


                // UPDATE
                // UPDATE
                // UPDATE

                sb.Append($@"
IF EXISTS (
    SELECT sys.procedures.name FROM sys.procedures WITH(NOLOCK) 
    WHERE type = 'P' AND schema_id = SCHEMA_ID('{schema}') AND name = '{dataset}__Update' 
) DROP PROCEDURE [{schema}].[{dataset}__Update]

GO

CREATE PROCEDURE [{schema}].[{dataset}__Update]
(
    @Key1Val {typeKey1},");
                if (!string.IsNullOrWhiteSpace(key2Name))
                    sb.Append($@"
    @Key2Val {typeKey2},");
                sb.Append($@"
    {GetArgumentNameList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");
                sb.RemoveLastComma();
                sb.Append($@"
)
AS
BEGIN");

                if (HasIdentity(identityName) || subTables.Count > 0) {
                    sb.Append($@"
    DECLARE @__IDENTITY int
");
                }

                sb.Append($@"
    UPDATE {fullTableName}
    SET {GetSetList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");

                if (HasIdentity(identityName) || subTables.Count > 0) {
                    sb.Append($@"
    @__IDENTITY=[{GetIdentityNameOrDefault(identityName)}],
");
                }
                sb.RemoveLastComma();

                sb.Append($@"
    WHERE [{key1Name}] = @Key1Val");
                if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND [{key2Name}] = @Key2Val");
                if (siteIdentity > 0) sb.Append($@" AND [{SQLGenericBase.SiteColumn}] = @{SQLGen.ValSiteIdentity}");

                sb.Append($@"

    DECLARE @__ROWCOUNT int = @@ROWCOUNT;");

                if (subTables.Count > 0) {
                    sb.Append($@"
    IF @__ROWCOUNT > 0
    BEGIN
");
                }

                foreach (SubTableInfo subTable in subTables) {
                    List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                    sb.Add($@"
        DELETE FROM {sb.BuildFullTableName(dbName, schema, subTable.Name)} WITH(SERIALIZABLE) WHERE [{SQLBase.SubTableKeyColumn}] = @__IDENTITY ;

        INSERT INTO {sb.BuildFullTableName(dbName, schema, subTable.Name)} ({GetColumnNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Add: true, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");
                    sb.RemoveLastComma();
                    sb.Append($@")
        SELECT {GetValueNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");
                    sb.RemoveLastComma();
                    sb.Append($@"
        FROM @arg{subTable.PropInfo.Name}
");
                }

                if (subTables.Count > 0) {
                    sb.Append($@"
    END");
                }

                sb.Append($@"
    SELECT @__ROWCOUNT  --- result set
END

GO

");

                // UPDATE BY IDENTITY
                // UPDATE BY IDENTITY
                // UPDATE BY IDENTITY

                if (HasIdentity(identityName)) {

                    sb.Append($@"
IF EXISTS (
    SELECT sys.procedures.name FROM sys.procedures WITH(NOLOCK) 
    WHERE type = 'P' AND schema_id = SCHEMA_ID('{schema}') AND name = '{dataset}__UpdateByIdentity' 
) DROP PROCEDURE [{schema}].[{dataset}__UpdateByIdentity]

GO

CREATE PROCEDURE [{schema}].[{dataset}__UpdateByIdentity]
(
    {GetArgumentNameList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: false, WithDerivedInfo: false, SubTable: false)}@valIdentity int,");
                    sb.RemoveLastComma();
                    sb.Append($@"
)
AS
BEGIN

    DECLARE @__IDENTITY int = @valIdentity
");

                    sb.Append($@"
    UPDATE {fullTableName}
    SET {GetSetList(dbName, schema, dataset, propData, objType, Prefix: null, TopMost: true, SiteSpecific: false, WithDerivedInfo: false, SubTable: false)}");
                    sb.RemoveLastComma();

                    sb.Append($@"
    WHERE [{identityName}] = @__IDENTITY");

                    sb.Append($@"

    DECLARE @__ROWCOUNT int = @@ROWCOUNT;");

                    if (subTables.Count > 0) {
                        sb.Append($@"
    IF @__ROWCOUNT > 0
    BEGIN
");
                    }

                    foreach (SubTableInfo subTable in subTables) {
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subTable.Type);
                        sb.Add($@"
        DELETE FROM {sb.BuildFullTableName(dbName, schema, subTable.Name)} WITH(SERIALIZABLE) WHERE [{SQLBase.SubTableKeyColumn}] = @__IDENTITY ;

        INSERT INTO {sb.BuildFullTableName(dbName, schema, subTable.Name)} ({GetColumnNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Add: true, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");
                        sb.RemoveLastComma();
                        sb.Append($@")
        SELECT {GetValueNameList(dbName, schema, subTable.Name, subPropData, subTable.Type, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: true)}");
                        sb.RemoveLastComma();
                        sb.Append($@"
        FROM @arg{subTable.PropInfo.Name}
");
                    }

                    if (subTables.Count > 0) {
                        sb.Append($@"
    END");
                    }

                    sb.Append($@"

    SELECT @__ROWCOUNT  --- result set
END

GO

");
                }

                // REMOVE
                // REMOVE
                // REMOVE

                sb.Append($@"
IF EXISTS (
    SELECT sys.procedures.name FROM sys.procedures WITH(NOLOCK) 
    WHERE type = 'P' AND schema_id = SCHEMA_ID('{schema}') AND name = '{dataset}__Remove' 
) DROP PROCEDURE [{schema}].[{dataset}__Remove]

GO

CREATE PROCEDURE [{schema}].[{dataset}__Remove]
(
    @Key1Val { typeKey1},");
                if (!string.IsNullOrWhiteSpace(key2Name))
                    sb.Append($@"
    @Key2Val {typeKey2},");
                if (siteIdentity > 0)
                    sb.Append($@"
    @{SQLGen.ValSiteIdentity} integer,");
                sb.RemoveLastComma();

                sb.Append($@"
)
AS
BEGIN");

                if (subTables.Count == 0) {

                    sb.Append($@"
    DELETE FROM {fullTableName}
    WHERE [{key1Name}] = @Key1Val");
                    if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND [{key2Name}] = @Key2Val");
                    if (siteIdentity > 0) sb.Append($@" AND [{SQLGenericBase.SiteColumn}] = @{SQLGen.ValSiteIdentity}");

                    sb.Append($@"

    SELECT @@ROWCOUNT --- result set
");

                } else {

                    sb.Append($@"
    DECLARE @__IDENTITY integer;

    SELECT @__IDENTITY = [{GetIdentityNameOrDefault(identityName)}] FROM {fullTableName}
    WHERE [{key1Name}] = @Key1Val");
                    if (!string.IsNullOrWhiteSpace(key2Name)) sb.Append($@" AND [{key2Name}] = @Key2Val");
                    if (siteIdentity > 0) sb.Append($@" AND [{SQLGenericBase.SiteColumn}] = @{SQLGen.ValSiteIdentity}");
                    sb.Append($@"
;");

                    foreach (SubTableInfo subTable in subTables) {
                        sb.Add($@"
    DELETE FROM {sb.BuildFullTableName(dbName, schema, subTable.Name)} WHERE {sb.BuildFullColumnName(subTable.Name, SQLGenericBase.SubTableKeyColumn)} = @__IDENTITY
;");
                    }
                    sb.Add($@"
    DELETE FROM {fullTableName} WHERE [{GetIdentityNameOrDefault(identityName)}] = @__IDENTITY

    SELECT @@ROWCOUNT --- result set
;");

                }

                sb.Append($@"
END

GO 

");

                // REMOVE BY IDENTITY
                // REMOVE BY IDENTITY
                // REMOVE BY IDENTITY

                if (HasIdentity(identityName)) {

                    sb.Append($@"
IF EXISTS (
    SELECT sys.procedures.name FROM sys.procedures WITH(NOLOCK) 
    WHERE type = 'P' AND schema_id = SCHEMA_ID('{schema}') AND name = '{dataset}__RemoveByIdentity' 
) DROP PROCEDURE [{schema}].[{dataset}__RemoveByIdentity]

GO

CREATE PROCEDURE [{schema}].[{dataset}__RemoveByIdentity]
(
    @valIdentity integer
)
AS
BEGIN");

                    sb.Append($@"
    DECLARE @__IDENTITY integer = @valIdentity;

;");
                    foreach (SubTableInfo subTable in subTables) {
                        sb.Add($@"
    DELETE FROM {sb.BuildFullTableName(dbName, schema, subTable.Name)} WHERE {sb.BuildFullColumnName(subTable.Name, SQLGenericBase.SubTableKeyColumn)} = @__IDENTITY
;");
                    }
                    sb.Add($@"
    DELETE FROM {fullTableName} WHERE [{GetIdentityNameOrDefault(identityName)}] = @__IDENTITY

    SELECT @@ROWCOUNT --- result set
;");

                    sb.Append($@"
END

GO 

");
                }

                // Add to database
                await ExecuteBatchesAsync(sb.ToString());
            }
            return true;
        }

        private void ExecuteBatches(string commandText) {
            ExecuteBatchesAsync(commandText).Wait();
        }
        private async Task ExecuteBatchesAsync(string commandText) {

            List<string> batches = reGo.Split(commandText).ToList();

            using (SqlCommand cmd = new SqlCommand()) {

                foreach (string batch in batches) {

                    if (!string.IsNullOrWhiteSpace(batch)) {
                        cmd.Connection = Conn;
                        cmd.CommandText = batch;
                        cmd.CommandType = System.Data.CommandType.Text;

                        YetaWF.Core.Log.Logging.AddTraceLog(cmd.CommandText);
                        if (YetaWFManager.IsSync())
                            cmd.ExecuteNonQuery();
                        else
                            await cmd.ExecuteNonQueryAsync();
                    }
                }
            }
        }
        private Regex reGo = new Regex(@"^\s*GO\s*$", RegexOptions.Compiled | RegexOptions.Multiline);

        internal void DropFunctions(string dbName, string schema, string dataset) {

            SQLBuilder sb = new SQLBuilder();

            sb.Append($@"
            {DropFunction(schema, $"{dataset}__Get")}
            {DropFunction(schema, $"{dataset}__GetByIdentity")}
            {DropFunction(schema, $"{dataset}__Add")}
            {DropFunction(schema, $"{dataset}__Update")}
            {DropFunction(schema, $"{dataset}__UpdateByIdentity")}
            {DropFunction(schema, $"{dataset}__Remove")}
            {DropFunction(schema, $"{dataset}__RemoveByIdentity")}");

            ExecuteBatches(sb.ToString());
        }

        private string DropFunction(string schema, string funcName) {

            SQLBuilder sb = new SQLBuilder();

            sb.Append($@"
IF EXISTS (
    SELECT sys.procedures.name FROM sys.procedures WITH(NOLOCK) 
    WHERE type = 'P' AND schema_id = SCHEMA_ID('{schema}') AND name = '{funcName}' 
) DROP PROCEDURE [{schema}].[{funcName}]");

            return sb.ToString();
        }

        internal static async Task<string?> CalculatedPropertiesAsync(Type objType, Func<string, Task<string>> calculatedPropertyCallbackAsync) {
            if (calculatedPropertyCallbackAsync == null) return null;
            SQLBuilder sb = new SQLBuilder();
            List<PropertyData> props = ObjectSupport.GetPropertyData(objType);
            props = (from p in props where p.CalculatedProperty select p).ToList();
            foreach (PropertyData prop in props) {
                string calcProp = await calculatedPropertyCallbackAsync(prop.Name);
                sb.Add($@"({calcProp}) AS [{prop.Name}],");
            }
            return sb.ToString();
        }

        protected string GetIdentityNameOrDefault(string identityName) {
            if (string.IsNullOrWhiteSpace(identityName))
                identityName = SQLGenericBase.IdentityColumn;
            return identityName;
        }

        internal string GetArgumentNameList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, string? Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            SQLManager sqlManager = new SQLManager();
            return ProcessColumns(
                (prefix, container, prop) => { // prop
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    string colType = GetDataTypeArgumentString(col);
                    return $@"@arg{prefix}{prop.Name} {colType},";
                },
                (prefix, container, prop) => {
                    return null;
                }, // Identity
                (prefix, container, prop) => { // binary
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    string colType = GetDataTypeArgumentString(col);
                    return $@"@arg{prefix}{prop.Name} {colType},";
                },
                (prefix, container, prop) => { // image
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    string colType = GetDataTypeArgumentString(col);
                    return $@"@arg{prefix}{prop.Name} {colType},";
                },
                (prefix, container, prop) => { // multistring
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sb = new StringBuilder();
                    foreach (LanguageData lang in Languages) {
                        string colName = $"{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, $"{prefix}{prop.Name}")}";
                        Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, colName);
                        string colType = GetDataTypeArgumentString(col);
                        sb.Append($@"@arg{colName} {colType},");
                    }
                    return sb.ToString();
                },
                (prefix, container, name) => { // predef
                    if (name == SQLGenericBase.SubTableKeyColumn)
                        return null;
                    if (name == SQLGen.DerivedTableName)
                        return $@"@{SQLGen.ValDerivedTableName} nvarchar(80),";
                    if (name == SQLGen.DerivedDataType)
                        return $@"@{SQLGen.ValDerivedDataType} nvarchar(200),";
                    if (name == SQLGen.DerivedAssemblyName)
                        return $@"@{SQLGen.ValDerivedAssemblyName} nvarchar(200),";
                    if (name == SQLGenericBase.SiteColumn) {
                        name = SQLGen.ValSiteIdentity;
                        return $@"@{prefix}{name} int,";
                    }
                    return null;
                },
                (prefix, container, prop, subPropData, subType, subtableName) => { // Subtable
                    return $@"@arg{prefix}{prop.ColumnName} AS [{schema}].[{subtableName}_T] READONLY,";
                },
                dbName, schema, dataset, null, propData, tpContainer, Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal string GetTypeNameList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, string? Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            SQLManager sqlManager = new SQLManager();
            return ProcessColumns(
                (prefix, container, prop) => {
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    string colType = GetDataTypeArgumentString(col);
                    return $@"[{prefix}{prop.Name}] {colType}{GetNullable(col)},";
                },
                (prefix, container, prop) => {
                    if (!SubTable) {
                        Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                        string colType = GetDataTypeArgumentString(col);
                        return $@"[{prefix}{prop.Name}] {colType}{GetNullable(col)},";
                    }
                    return null;
                }, // Identity
                (prefix, container, prop) => {
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    string colType = GetDataTypeArgumentString(col);
                    return $@"[{prefix}{prop.Name}] {colType}{GetNullable(col)},";
                },
                (prefix, container, prop) => {
                    Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    string colType = GetDataTypeArgumentString(col);
                    return $@"[{prefix}{prop.Name}] {colType}{GetNullable(col)},";
                },
                (prefix, container, prop) => {
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sb = new StringBuilder();
                    foreach (LanguageData lang in Languages) {
                        string colName = $"{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, $"{prefix}{prop.Name}")}";
                        Column col = sqlManager.GetColumn(Conn, dbName, schema, dataset, colName);
                        string colType = GetDataTypeArgumentString(col);
                        return $@"[{colName}] {colType}{GetNullable(col)},";
                    }
                    return sb.ToString();
                },
                (prefix, container, name) => { // predef
                    if (name == SQLGenericBase.SubTableKeyColumn)
                        return null;
                    if (name == SQLGenericBase.SiteColumn) {
                        name = SQLGen.ValSiteIdentity;
                        return $@"[{prefix}{name}] int NOT NULL,";
                    }
                    return null;
                },
                (prefix, container, prop, subPropData, subType, subtableName) => { // Subtable
                    return null;
                },
                dbName, schema, dataset, null, propData, tpContainer, Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal string GetColumnNameList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, bool Add = false, string? Prefix = null, bool TopMost = true, string? IdentityName = null, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false,
                Dictionary<string, string>? VisibleColumns = null) {
            SQLBuilder sb = new SQLBuilder();
            SQLManager sqlManager = new SQLManager();
            return ProcessColumns(
                (prefix, container, prop) => {
                    string col = $@"{prefix}{prop.ColumnName}";
                    string fullCol = sb.BuildFullColumnName(dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    if (VisibleColumns != null) {
                        if (VisibleColumns.ContainsKey(col)) return null;
                        VisibleColumns.Add($"{prefix}{prop.ColumnName}", fullCol);
                    }
                    return $"{fullCol},";
                },
                (prefix, container, prop) => { // Identity
                    if (Add) {
                        return null;
                    } else {
                        string col = $@"{prefix}{prop.ColumnName}";
                        string fullCol = sb.BuildFullColumnName(dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                        if (VisibleColumns != null) {
                            if (VisibleColumns.ContainsKey(col)) return null;
                            VisibleColumns.Add($"{prefix}{prop.ColumnName}", fullCol);
                        }
                        return $"{fullCol},";
                    }
                },
                (prefix, container, prop) => {
                    string col = $@"{prefix}{prop.ColumnName}";
                    string fullCol = sb.BuildFullColumnName(dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    if (VisibleColumns != null) {
                        if (VisibleColumns.ContainsKey(col)) return null;
                        VisibleColumns.Add($"{prefix}{prop.ColumnName}", fullCol);
                    }
                    return $"{fullCol},";
                },
                (prefix, container, prop) => {
                    string col = $@"{prefix}{prop.ColumnName}";
                    string fullCol = sb.BuildFullColumnName(dbName, schema, dataset, $"{prefix}{prop.ColumnName}");
                    if (VisibleColumns != null) {
                        if (VisibleColumns.ContainsKey(col)) return null;
                        VisibleColumns.Add($"{prefix}{prop.ColumnName}", fullCol);
                    }
                    return $"{fullCol},";
                },
                (prefix, container, prop) => {
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sbldr = new StringBuilder();
                    foreach (LanguageData lang in Languages) {
                        string col = $@"{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name)}";
                        string fullCol = sb.BuildFullColumnName(dbName, schema, dataset, col);
                        if (VisibleColumns != null) {
                            if (VisibleColumns.ContainsKey(col)) return null;
                            VisibleColumns.Add(col, fullCol);
                        }
                        sbldr.Append($"{fullCol},");
                    }
                    return sbldr.ToString();
                },
                (prefix, container, name) => { // predef
                    if (Add) {
                        string col = $@"{prefix}{name}";
                        string fullCol = sb.BuildFullColumnName(dbName, schema, dataset, $"{prefix}{name}");
                        if (VisibleColumns != null) {
                            if (VisibleColumns.ContainsKey(col)) return null;
                            VisibleColumns.Add($"{prefix}{name}", fullCol);
                        }
                        return $"{fullCol},";
                    } else {
                        return null;
                    }
                },
                (prefix, container, prop, subPropData, subType, subtableName) => { // Subtable
                    return null;
                },
                dbName, schema, dataset, null, propData, tpContainer, Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal string GetValueNameList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, string? Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            return ProcessColumns(
                (prefix, container, prop) => {
                    if (SubTable)
                        return $@"[{prop.Name}],";
                    else
                        return $@"@arg{prefix}{prop.Name},";
                },
                (prefix, container, prop) => { return null; }, // Identity
                (prefix, container, prop) => { // Binary
                    if (SubTable) {
                        return $@"[{prop.Name}],";
                    } else
                        return $@"@arg{prefix}{prop.Name},";
                },
                (prefix, container, prop) => { // Image
                    if (SubTable) {
                        return $@"[{prop.Name}],";
                    } else
                        return $@"@arg{prefix}{prop.Name},";
                },
                (prefix, container, prop) => { // Language
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sb = new StringBuilder();
                    foreach (LanguageData lang in Languages) {
                        sb.Append($@"@arg{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name)},");
                    }
                    return sb.ToString();
                },
                (prefix, container, name) => { // predef
                    if (name == SQLGen.DerivedTableName)
                        return $@"@{SQLGen.ValDerivedTableName},";
                    if (name == SQLGen.DerivedDataType)
                        return $@"@ValDerivedDataType,";
                    if (name == SQLGen.DerivedAssemblyName)
                        return $@"@{SQLGen.ValDerivedAssemblyName},";
                    if (name == SQLGenericBase.SiteColumn)
                        return $@"@{SQLGen.ValSiteIdentity},";
                    if (name == SQLGenericBase.SubTableKeyColumn)
                        return $"@__IDENTITY,";
                    return $@"@arg{prefix}{name},";
                },
                (prefix, container, prop, subPropData, subType, subtableName) => { // Subtable
                    return null;
                },
                dbName, schema, dataset, null, propData, tpContainer, Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal string GetSetList(string dbName, string schema, string dataset, List<PropertyData> propData, Type tpContainer, bool Add = false, string? Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {
            if (SubTable) throw new InternalError($"{nameof(GetSetList)} called for subtable which is not supported");
            return ProcessColumns(
                (prefix, container, prop) => { return $@"[{prefix}{prop.ColumnName}]=@arg{prefix}{prop.Name},"; },
                (prefix, container, prop) => { return null; }, // Identity
                (prefix, container, prop) => { return $@"[{prefix}{prop.ColumnName}]=@arg{prefix}{prop.Name},"; },
                (prefix, container, prop) => { return $@"[{prefix}{prop.ColumnName}]=@arg{prefix}{prop.Name},"; },
                (prefix, container, prop) => {
                    if (Languages.Count == 0) throw new InternalError("We need Languages for MultiString support");
                    StringBuilder sb = new StringBuilder();
                    foreach (LanguageData lang in Languages)
                        sb.Append($@"[{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name)}]=@arg{prefix}{SQLGenericBase.ColumnFromPropertyWithLanguage(lang.Id, prop.Name)},");
                    return sb.ToString();
                },
                (prefix, container, name) => {
                    return null;
                },
                (prefix, container, prop, subPropData, subType, subtableName) => {
                    return null;
                },
                dbName, schema, dataset, null, propData, tpContainer, Prefix, TopMost, SiteSpecific, WithDerivedInfo, SubTable);
        }
        internal static string ProcessColumns(
                Func<string?, object?, PropertyData, string?> fmt,
                Func<string?, object?, PropertyData, string?> fmtIdentity,
                Func<string?, object?, PropertyData, string?> fmtBinary,
                Func<string?, object?, PropertyData, string?> fmtImage,
                Func<string?, object?, PropertyData, string?> fmtLanguage,
                Func<string?, object?, string, string?> fmtPredef,
                Func<string?, object?, PropertyData, List<PropertyData>, Type, string, string?> fmtSubtable,
                string dbName, string schema, string dataset,
                object? container, List<PropertyData> propData, Type tpContainer,
                string? Prefix = null, bool TopMost = true, bool SiteSpecific = false, bool WithDerivedInfo = false, bool SubTable = false) {

            SQLBuilder sb = new SQLBuilder();
            foreach (PropertyData prop in propData) {
                Type propertyType = prop.PropInfo.PropertyType;
                if (prop.PropInfo.CanRead && prop.PropInfo.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    string colName = prop.ColumnName;
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
                                .Select(t => t.GetGenericArguments()[0]).First();
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(subType);
                        string subTableName = dataset + "_" + prop.Name;
                        sb.Add(fmtSubtable(Prefix, container, prop, subPropData, subType, subTableName));
                    } else if (propertyType.IsClass) {
                        object? sub = null;
                        if (container != null)
                            sub = prop.PropInfo.GetValue(container);
                        List<PropertyData> subPropData = ObjectSupport.GetPropertyData(propertyType);
                        string args = ProcessColumns(fmt, fmtIdentity, fmtBinary, fmtImage, fmtLanguage, fmtPredef, fmtSubtable, dbName, schema, dataset, sub, subPropData, propertyType, Prefix + prop.Name + "_", TopMost, SiteSpecific: false, WithDerivedInfo: false, SubTable: false);
                        sb.Add(args);
                    } else
                        throw new InternalError($"Unknown property type {propertyType.FullName} used in class {tpContainer.FullName}, property {colName}");
                }
            }
            if (SiteSpecific)
                sb.Add(fmtPredef(Prefix, container, SQLGenericBase.SiteColumn));
            if (WithDerivedInfo) {
                sb.Add(fmtPredef(Prefix, container, SQLGen.DerivedTableName));
                sb.Add(fmtPredef(Prefix, container, SQLGen.DerivedDataType));
                sb.Add(fmtPredef(Prefix, container, SQLGen.DerivedAssemblyName));
            }
            if (SubTable)
                sb.Add(fmtPredef(Prefix, container, SQLGenericBase.SubTableKeyColumn));
            return sb.ToString();
        }
    }
}