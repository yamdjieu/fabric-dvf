CREATE TABLE [gold].[dim_commune] (
    [code_commune]     VARCHAR (8000) NULL,
    [nom_commune]      VARCHAR (8000) NULL,
    [code_departement] VARCHAR (8000) NULL,
    [nom_departement]  VARCHAR (8000) NULL,
    [code_region]      VARCHAR (8000) NULL,
    [nom_region]       VARCHAR (8000) NULL,
    [population]       BIGINT         NULL,
    [est_historique]   BIT            NULL
);


GO