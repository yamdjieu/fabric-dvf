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
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.option("header", True).csv("Files/bronze/dvf/2021/full.csv.gz")
display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()
print(df.count(), "lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.groupBy("nature_mutation").count().orderBy("count", ascending=False))
display(df.groupBy("type_local").count().orderBy("count", ascending=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

multi = df.groupBy("id_mutation").count().filter(F.col("count")>1)
print (multi.count(), "mutations sur plusieurs lignes")
display(multi.orderBy(F.col("count").desc()).limit(5)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.filter(F.col("id_mutation") == "2021-687212"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.select("id_mutation", "date_mutation", "nom_commune", "valeur_fonciere").limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nb_ligne = df.filter(
    (F.col("nature_mutation") == "Vente") & (F.col("type_local") == "Appartement")
)
print(nb_ligne.count(), "lignes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.groupBy("type_local").count().orderBy("count", ascending=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

top10 = (
    df
    .filter((F.col("nature_mutation") == "Vente") & (F.col("type_local") == "Appartement"))
    .groupBy("nom_commune")
    .count()
    .orderBy("count", ascending=False)
    .limit(10)
)
display(top10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
