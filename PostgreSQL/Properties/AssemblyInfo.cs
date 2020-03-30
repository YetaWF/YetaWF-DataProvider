/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System.Reflection;
using YetaWF.PackageAttributes;

[assembly: AssemblyTitle("PostgreSQL")]
[assembly: AssemblyDescription("YetaWF PostgreSQL Data Provider")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Softel vdm, Inc.")]
[assembly: AssemblyCopyright("Copyright © 2020 - Softel vdm, Inc.")]
[assembly: AssemblyProduct("PostgreSQL")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

[assembly: AssemblyVersion("5.1.0.0")]

[assembly: Package(PackageTypeEnum.DataProvider, "YetaWF")]


// PostgreSQL is not well suited for YetaWF. We need case insensitive collation, so grid filters, etc. work "better". Translating to lowercase in queries 
// is not an option as it is s-l-o-w. And in general having the same keys allowed even if they only differ in casing feels just wrong.
// Postgres 12 has case insensitive collation, but it does NOT work with LIKE. At all.
//
// Example of case insensitive collation:
//
//CREATE COLLATION public.ignorecase
//    (LC_COLLATE = '@colStrength=secondary', LC_CTYPE = '@colStrength=secondary');
//
// Table using it:
//
//CREATE TABLE public."TEST"
//(
//    "Name" character varying(100) COLLATE public.ignorecase
//)
//
// Sadly, not working with LIKE is a deal breaker. So instead we'll use case sensitive (default) collation and suck it up.
// The areas that have a problem with this (e.g., Identity) are modified to save items (like name, email address) in lowercase, or whatever works best.
// 
// Hopefully a PostgreSQL version in the near future removes the "LIKE" limitation.