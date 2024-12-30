Tento notebook představuje druhou fázi ETL procesu a má za úkol vyčistit a transformovat získaná data z bronzového notebooku.

from pyspark.sql.functions import col, from_unixtime, to_timestamp
from delta.tables import DeltaTable


Začal jsem načtením mojeho JSON souboru do spark dataframe kvůli lepšímu data processingu při větším množství dat a poté si přejmenoval jednotlivé atributy podle toho, jak potřebuji:

# Read the Carbon Intensity JSON data into a Spark DataFrame
df = spark.read.option("multiline", "true").json(f"Files/carbon_emission_UK_data.json")

df = (
    df.select(
        col("from").alias("time_from"),
        col("to").alias("time_to"),
        col("intensity.forecast").alias("forecast_intensity"),
        col("intensity.actual").alias("actual_intensity"),
        col("intensity.index").alias("intensity_index")
    )
)

df.show(n=200, truncate=False)

*df.show(n=200, truncate=False) slouží k debuggingu aby v případě problému šlo snadněji odhalit potencionální problémy (duplikace, korupce dat,..)

A poté naformátuji časové udaje do datumového data typu:

df = (
    df.withColumn("time_from", to_timestamp(col("time_from"), "yyyy-MM-dd'T'HH:mm'Z'"))
      .withColumn("time_to", to_timestamp(col("time_to"), "yyyy-MM-dd'T'HH:mm'Z'"))
)

df.show(n=200, truncate=False)

Na základě časových dat odstraním případné duplikáty:

df = df.dropDuplicates(["time_from", "time_to"])


Vytvořil jsem si z těchto upravených dat tabulku:

table_name = "carbon_emission_silver"

A v poseldní řadě vytvořil finálné silver tabulku pokud neexistuje a nebo sloučit nová a stará data přepsat je do tabulky:

# Check if the Delta table already exists and perform a merge to add only new data
if DeltaTable.isDeltaTable(spark, table_name):
    delta_table = DeltaTable.forName(spark, table_name)
    delta_table.alias("tgt").merge(
        df.alias("src"),
        "tgt.time_from = src.time_from AND tgt.time_to = src.time_to"  # Matching condition
    ).whenNotMatchedInsertAll().execute()
else:
    # If the table doesn't exist, create it
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

