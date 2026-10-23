CREATE TABLE [gold].[dim_type_bien] (
    [type_bien_key] BIGINT         NULL,
    [type_local]    VARCHAR (8000) NULL,
    [est_neuf]      BIT            NULL,
    [etat]          VARCHAR (6)    NOT NULL
);


GO