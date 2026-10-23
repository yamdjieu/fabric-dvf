CREATE TABLE [gold].[dim_date] (
    [date_key]   INT          NULL,
    [date_jour]  DATE         NULL,
    [annee]      INT          NULL,
    [trimestre]  INT          NULL,
    [mois]       INT          NULL,
    [annee_mois] VARCHAR (26) NOT NULL
);


GO