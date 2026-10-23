CREATE TABLE [gold].[fact_ventes] (
    [id_mutation]         VARCHAR (8000) NULL,
    [date_key]            INT            NULL,
    [code_commune]        VARCHAR (8000) NULL,
    [type_bien_key]       BIGINT         NULL,
    [valeur_fonciere]     FLOAT (53)     NULL,
    [surface_reelle_bati] FLOAT (53)     NULL,
    [prix_m2]             FLOAT (53)     NULL,
    [nombre_pieces]       INT            NULL,
    [longitude]           FLOAT (53)     NULL,
    [latitude]            FLOAT (53)     NULL
);


GO