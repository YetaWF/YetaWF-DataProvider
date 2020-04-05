
IF EXISTS (
    SELECT sys.procedures.name FROM sys.procedures WITH(NOLOCK) 
    WHERE type = 'P' AND schema_id = SCHEMA_ID('[Var,SQL-Dbo]') AND name = 'YetaWF_Modules__GetBase' 
) DROP PROCEDURE [Var,SQL-Dbo].[YetaWF_Modules__GetBase]

GO

CREATE PROCEDURE [Var,SQL-Dbo].[YetaWF_Modules__GetBase]
(
    @Key1Val uniqueidentifier,
    @valSiteIdentity integer
)
AS
BEGIN
    DECLARE @Table nvarchar(80);
    DECLARE @Type nvarchar(200);
    DECLARE @Asm nvarchar(200);

    SELECT TOP 1 @Table=[DerivedDataTableName], @Type=[DerivedDataType], @Asm=[DerivedAssemblyName]
    FROM [yetawf].[dbo].[YetaWF_Modules] WITH(NOLOCK)
    WHERE [ModuleGuid] = @Key1Val AND [__Site] = @valSiteIdentity
    
    IF @@ROWCOUNT > 0
    BEGIN

        SELECT @Table as [DerivedTableName], @Type as [DerivedDataType], @Asm as [DerivedAssemblyName]
; --- result set

        DECLARE @Sproc nvarchar(200) = '[dbo].[' + @Table + '__Get]';

        EXEC @Sproc @Key1Val=@Key1Val,@valSiteIdentity=@valSiteIdentity
; --- result set

    END
END

GO
