/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections.Generic;
using System.Linq;
using YetaWF.Core.Models;
using YetaWF.DataProvider.SQLGeneric;

namespace YetaWF.DataProvider.SQL {

    internal partial class SQLGen {

        internal const string ValDerivedTableName = "valDerivedTableName"; // sproc argument for derived table name
        internal const string ValDerivedDataType = "valDerivedDataType"; // sproc argument for derived type 
        internal const string ValDerivedAssemblyName = "valDerivedAssemblyName"; // sproc argument for derived assembly name

        internal const string DerivedTableName = "DerivedDataTableName"; // column names
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
IF EXISTS (
    SELECT sys.procedures.name FROM sys.procedures WITH(NOLOCK) 
    WHERE type = 'P' AND schema_id = SCHEMA_ID('{schema}') AND name = '{dataset}__Get' 
) DROP PROCEDURE [{schema}].[{dataset}__Get]

GO

CREATE PROCEDURE [{schema}].[{dataset}__Get]
(
    @Key1Val {typeKey1},");
            if (siteIdentity > 0)
                sb.Append($@"
    @{SQLGen.ValSiteIdentity} integer,");

            sb.RemoveLastComma();
            sb.Append($@"
)
AS
BEGIN");

            Dictionary<string, string> visibleColumns = new Dictionary<string, string>();

            sb.Append($@"
    SELECT TOP 1 {GetColumnNameList(dbName, schema, baseDataset, basePropData, baseType, Prefix: null, TopMost: false, SiteSpecific: true, WithDerivedInfo: false, SubTable: false, VisibleColumns: visibleColumns)}{GetColumnNameList(dbName, schema, dataset, propDataNoDups, type, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: false, VisibleColumns: visibleColumns)}");
            sb.RemoveLastComma();

            sb.Append($@"
    FROM {fullBaseTableName} WITH(NOLOCK)
    LEFT JOIN {fullTableName} ON {fullBaseTableName}.[{key1Name}] = {fullTableName}.[{key1Name}] AND {fullBaseTableName}.[{SQLGenericBase.SiteColumn}] = {fullTableName}.[{SQLGenericBase.SiteColumn}]
    WHERE {fullBaseTableName}.[{key1Name}] = @Key1Val AND {fullBaseTableName}.[{SQLGenericBase.SiteColumn}] = @{SQLGen.ValSiteIdentity}
; --- result set

END

GO 

");

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
    {GetArgumentNameList(dbName, schema, baseDataset, basePropData, baseType, Prefix: null, TopMost: false, SiteSpecific: true, WithDerivedInfo: true, SubTable: false)}{GetArgumentNameList(dbName, schema, dataset, propDataNoDups, type, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: false)}");
            sb.RemoveLastComma();
            sb.Append($@"
)
AS
BEGIN
    INSERT INTO {fullBaseTableName} ({GetColumnNameList(dbName, schema, baseDataset, basePropData, baseType, Add: true, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: true, SubTable: false)}");
            sb.RemoveLastComma();
            sb.Append($@")
    VALUES({GetValueNameList(dbName, schema, baseDataset, basePropData, baseType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: true, SubTable: false)}");
            sb.RemoveLastComma();
            sb.Append($@")
;
    DECLARE @__ROWCOUNT int = @@ROWCOUNT;
    SELECT @__ROWCOUNT  --- result set
;");

            sb.Append($@"

    IF @__ROWCOUNT IS NOT NULL
    BEGIN
        INSERT INTO {fullTableName} ({GetColumnNameList(dbName, schema, dataset, propData, type, Add: true, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");
            sb.RemoveLastComma();
            sb.Append($@")
        VALUES({GetValueNameList(dbName, schema, dataset, propData, type, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");
            sb.RemoveLastComma();
            sb.Append($@")
    END
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
    {GetArgumentNameList(dbName, schema, baseDataset, basePropData, baseType, Prefix: null, TopMost: false, SiteSpecific: true, WithDerivedInfo: true, SubTable: false)}{GetArgumentNameList(dbName, schema, dataset, propDataNoDups, type, Prefix: null, TopMost: false, SiteSpecific: false, WithDerivedInfo: false, SubTable: false)}");
            sb.RemoveLastComma();
            sb.Append($@"
)
AS
BEGIN");

            basePropData = (from p in basePropData where p.Name != key1Name select p).ToList();// remove ModuleGuid from set list, module guid doesn't change
            propData = (from p in propData where p.Name != key1Name select p).ToList();// remove ModuleGuid from set list

            sb.Append($@"
    UPDATE {fullBaseTableName}
    SET {GetSetList(dbName, schema, baseDataset, basePropData, baseType, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false)}");
            sb.RemoveLastComma();
            sb.Append($@"
    WHERE [{key1Name}] = @arg{key1Name} AND [{SQLGenericBase.SiteColumn}] = @{SQLGen.ValSiteIdentity}
;
    DECLARE @__ROWCOUNT int = @@ROWCOUNT;
    SELECT @@ROWCOUNT  --- result set
;");

            string setList = GetSetList(dbName, schema, dataset, propData, type, Prefix: null, TopMost: true, SiteSpecific: siteIdentity > 0, WithDerivedInfo: false, SubTable: false);
            if (!string.IsNullOrWhiteSpace(setList)) {

                sb.Append($@"

    IF @__ROWCOUNT IS NOT NULL
    BEGIN
        UPDATE {fullTableName}
        SET {setList}");
                sb.RemoveLastComma();

                sb.Append($@"
        WHERE [{key1Name}] = @arg{key1Name} AND [{SQLGenericBase.SiteColumn}] = @{SQLGen.ValSiteIdentity}
    END
;");
            }

            sb.Append($@"
END

GO

");

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
    @Key1Val {typeKey1},");
            if (siteIdentity > 0)
                sb.Append($@"
    @{SQLGen.ValSiteIdentity} integer,");

            sb.RemoveLastComma();
            sb.Append($@"
)
AS
BEGIN
    DELETE FROM {fullBaseTableName}
    WHERE [{key1Name}] = @Key1Val AND [{SQLGenericBase.SiteColumn}] = @{SQLGen.ValSiteIdentity}
;
    SELECT @@ROWCOUNT --- result set
;

    DELETE FROM {fullTableName}
    WHERE [{key1Name}] = @Key1Val AND [{SQLGenericBase.SiteColumn}] = @{SQLGen.ValSiteIdentity}
;

END

GO

");

            // Add to database
            ExecuteBatches(sb.ToString());

            return true;
        }
   }
}