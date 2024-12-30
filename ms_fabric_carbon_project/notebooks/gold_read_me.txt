Ve třetím gold notebooku, představujicí finální zpracování dat připravenou na analýzu jsem začal načtením stříbrné tabulky a vytvořením doačasné SQL tabulky:

from pyspark.sql.functions import col, unix_timestamp, lit, when, row_number
from pyspark.sql.window import Window

df_silver = spark.read.table("carbon_emission_silver")

# Register the Silver Layer DataFrame as a temporary SQL view
df_silver.createOrReplaceTempView("silver_view")

V této dočasné tabulce jsem si vybral metriky které chci dále využít, předchystal případné další užitečně sloupce a vytřídil záznamy které mají chybějicí data, se zobrazením jak tato tabulka vypadá pro připadný debugging. VYužil jsem zde SQL kvůli snažší přehlednosti a jednoduchosti v porovnání s PySpark:


# Select data for gold_table using SQL
df_gold_new = spark.sql("""
    SELECT 
        time_from,
        time_to,
        forecast_intensity,
        actual_intensity,
        intensity_index,
        (unix_timestamp(time_to) - unix_timestamp(time_from)) / 60 AS duration_minutes,
        CASE 
            WHEN actual_intensity IS NULL THEN 'low_quality' 
            ELSE 'good_quality' 
        END AS data_quality
    FROM silver_view
""")

df_gold_new.createOrReplaceTempView("gold_new_view")
df_gold_new.show(n=200, truncate=False)

v neposlední řade jsem znovu vytvořil tabulku pokud neexistuje a inkrementálně načetl nová data pokud existují. UNION DISTINCT v tomhle případe slouží ke sloučení nových a starých dat a vyfiltrování potencionálích duplikátů

# Combine existing and new data
try:
    df_gold_existing = spark.read.table("carbon_emission_gold")
    df_gold_existing.createOrReplaceTempView("gold_existing_view")

    df_gold_combined = spark.sql("""
        SELECT * FROM gold_existing_view
        UNION DISTINCT
        SELECT * FROM gold_new_view
    """)
except Exception as e:
    print("Existing gold table not found. Creating a new one.")
    df_gold_combined = df_gold_new

(v silver_ntb jsem použil na kontrolu existence tabulky IF a zde TRY/EXCEPT - v tomto projektu je to podle mě zaměnitelné a spíš jde o ukázku obou přístupů)

Na závěr seřazuji data na základě "time_from" a přidávám každémů zápisu id. Finálně tabulku ukládám. 

# Define a Window Specification, Assign a Row Number and Add a New Column
window_spec = Window.orderBy("time_from")
df_gold_combined = df_gold_combined.withColumn("id", row_number().over(window_spec))
df_gold_combined.write.mode("append").saveAsTable("carbon_emission_gold")