
CREATE OR REPLACE FUNCTION "[Var,PostgreSQL-Schema]"."Y__GetBase"(
    "Key1Val" uuid,
    "valSiteIdentity" integer)
    RETURNS SETOF "[Var,PostgreSQL-Schema]"."Y_DerivedInfo_T"
    LANGUAGE 'plpgsql'
AS $$
BEGIN
    RETURN QUERY(
        SELECT "DerivedTableName", "DerivedDataType", "DerivedAssemblyName"
        FROM "[Var,PostgreSQL-Schema]"."YetaWF_Modules"
        WHERE "ModuleGuid" = "Key1Val" AND "__Site" = "valSiteIdentity"
        LIMIT 1    --- result set
    );
END;
$$;
