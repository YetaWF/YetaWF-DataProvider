﻿

CREATE TYPE "[Var,PostgreSQL-Schema]"."Y_DerivedInfo_T" AS
(
	"DerivedTableName" character varying(80),
	"DerivedDataType" character varying(200),
	"DerivedAssemblyName" character varying(200)
);
