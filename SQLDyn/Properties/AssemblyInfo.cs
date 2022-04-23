/* Copyright © 2022 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System.Reflection;
using YetaWF.PackageAttributes;

[assembly: AssemblyTitle("SQLDyn")]
[assembly: AssemblyDescription("YetaWF SQLDyn Data Provider")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Softel vdm, Inc.")]
[assembly: AssemblyCopyright("Copyright © 2022 - Softel vdm, Inc.")]
[assembly: AssemblyProduct("SQLDyn")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

[assembly: AssemblyVersion("5.5.0.0")]

[assembly: Package(PackageTypeEnum.DataProvider, "YetaWF.DataProvider")]

[assembly: PackageInfo("https://YetaWF.com/UpdateServer",
    "https://yetawf.com/Documentation/YetaWFSQLDyn",
    "https://YetaWF.com/Documentation/YetaWFSQLDyn#Support",
    "https://yetawf.com/Documentation/YetaWFSQLDyn#Release%20Notice",
    "https://yetawf.com/Documentation/YetaWFSQLDyn#License")]

// TODO: $$$$ There seems to be a problem with subtables when the primary key doesn't coincide with the identity.
// This use case is not present in a standard YetaWF distribution, but may be used in some custom sites (unverified at this time).
// This needs to be addressed with extensive testing (as time permits), to be correctly supported.
// This affects SQL, SQLDyn and PostgreSQL data providers