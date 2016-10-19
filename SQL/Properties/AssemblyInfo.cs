/* Copyright © 2016 Softel vdm, Inc. - http://yetawf.com/Documentation/YetaWF/Licensing */

using System.Reflection;
using YetaWF.PackageAttributes;

[assembly: AssemblyTitle("SQL")]
[assembly: AssemblyDescription("YetaWF SQL Data Provider")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("Softel vdm, Inc.")]
[assembly: AssemblyCopyright("Copyright © 2016 - Softel vdm, Inc.")]
[assembly: AssemblyProduct("SQL")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

[assembly: AssemblyVersion("1.0.9.0")]

[assembly: Package(PackageTypeEnum.DataProvider, "YetaWF")]

// TODO: The SQL data providers are a hot mess and need some serious cleaning up, they work but aren't pretty