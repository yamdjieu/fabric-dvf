# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "936e953a-ecc6-43a8-9bcf-ea9f13c4a0f9",
# META       "default_lakehouse_name": "lh_bronze_silver",
# META       "default_lakehouse_workspace_id": "4bbeb02e-f4d8-434b-b238-91beaf6d15c4",
# META       "known_lakehouses": [
# META         {
# META           "id": "936e953a-ecc6-43a8-9bcf-ea9f13c4a0f9"
# META         }
# META       ]
# META     },
# META     "warehouse": {}
# META   }
# META }

# CELL ********************


#Connexion au lakehouse
from pyspark.sql import functions as F

df = spark.read.option("Header", True).csv("Files/bronze/dvf/*/full.csv.gz")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_typed = df.select(
    "id_mutation",
    F.to_date("date_mutation", "yyyy-MM-dd").alias("date_mutation"),
    "nature_mutation",
    F.col("valeur_fonciere").cast("double").alias("valeur_fonciere"),
    "code_commune",
    "nom_commune",
    "code_departement",
    "type_local",
    F.col("surface_reelle_bati").cast("double").alias("surface_reelle_bati"),
    F.col("nombre_pieces_principales").cast("int").alias("nombre_pieces"),
    F.col("longitude").cast("double").alias("longitude"),
    F.col("latitude").cast("double").alias("latitude"),
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_typed = df_typed.withColumn("annee", F.year("date_mutation"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

avant = df.filter(F.col("valeur_fonciere").isNull()).count()
apres = df_typed.filter(F.col("valeur_fonciere").isNull()).count()
#print("Prix vides avant :", avant, "| après :", apres)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VEFA = "Vente en l'état futur d'achèvement"

df_ventes = (
    df_typed
    .filter(F.col("nature_mutation").isin("Vente", VEFA))
    .withColumn("est_neuf", F.col("nature_mutation") == VEFA)

)
#print(df_typed.count(), "lignes →", df_ventes.count(), "lignes")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(df_ventes.groupBy("nature_mutation", "est_neuf").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(df_ventes.groupBy("type_local").count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

LOGEMENTS = ["Maison", "Appartement"]
LOCAL_PRO = "Local industriel. commercial ou assimilé"

stats = df_ventes.groupBy("id_mutation").agg(
    F.sum(F.col("type_local").isin(LOGEMENTS).cast("int")).alias("nb_logements"),
    F.sum((F.col("type_local") == LOCAL_PRO).cast("int")).alias("nb_locaux_pro"),
)

mutations_ok = stats.filter(
    (F.col("nb_logements") == 1)
    & (F.coalesce(F.col("nb_locaux_pro"), F.lit(0)) == 0)
).select("id_mutation")

#print(stats.count(), "mutations au total →", mutations_ok.count(), "retenues")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_logements = (
    df_ventes
    .join(mutations_ok, "id_mutation")
    .filter(F.col("type_local").isin(LOGEMENTS))
)

doublons = df_logements.groupBy("id_mutation").count().filter(F.col("count") > 1).count()
#print(df_logements.count(), "ventes | doublons restants :", doublons)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_prix = (
    df_logements
    .filter(F.col("valeur_fonciere").isNotNull())
    .filter(F.col("surface_reelle_bati") > 0)
    .withColumn("prix_m2", F.round(F.col("valeur_fonciere") / F.col("surface_reelle_bati"), 0))
)
#print(df_logements.count(), "ventes →", df_prix.count(), "avec un prix au m² calculable")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_prix.select("prix_m2", "valeur_fonciere", "surface_reelle_bati")
    .summary("min", "1%", "5%", "50%", "95%", "99%", "max")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver = df_prix.filter(
    F.col("surface_reelle_bati").between(9, 1000)
    & F.col("prix_m2").between(300, 25000)
)
print(df_prix.count(), "→", df_silver.count(), "ventes après filtrage des aberrations")
# display(df_silver.select("prix_m2", "surface_reelle_bati").summary("min", "50%", "max"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

(
    df_silver.write
    .mode("overwrite")
    .format("delta")
    .partitionBy("annee")
    .saveAsTable("silver.mutations")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
    SELECT annee,
           COUNT(*) AS nb_ventes,
           PERCENTILE_APPROX(prix_m2, 0.5) AS prix_m2_median,
           SUM(CAST(est_neuf AS INT)) AS nb_neuf
    FROM silver.mutations
    GROUP BY annee
    ORDER BY annee
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
