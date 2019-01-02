/* Copyright © 2019 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

namespace YetaWF.DataProvider.SQL {
    public interface ISQLTableInfo {
        string GetConnectionString();
        string GetDatabaseName();
        string GetDbOwner();
        string GetTableName();
        string ReplaceWithTableName(string text, string searchText);
        string ReplaceWithLanguage(string text, string searchText);
    }
}
