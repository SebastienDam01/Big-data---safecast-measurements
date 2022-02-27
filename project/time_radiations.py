import matplotlib.pyplot as plt
import pyspark
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring


print("Spark lancé !")

conf = (
    pyspark.SparkConf()
    .setMaster("local[*]")
    .setAll(
        [
            ("spark.executor.memory", "1g"),  # find
            ("spark.driver.memory", "1g"),  # your
            ("spark.driver.maxResultSize", "1g"),  # setup
        ]
    )
)
# create the session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# create the context
sc = spark.sparkContext

df = spark.read.csv("data/measurements.csv", header=True)
df_sample = df.sample(withReplacement=None, fraction=0.08, seed=42)
df_sample.createOrReplaceTempView("radiations")

print("Nombre de mesures étudiées dans l'échantillon:", df_sample.count())

print("Affichage des radiations mesurées en fonction de l'année...")

results = spark.sql(
    """
    SELECT
        YEAR(`Captured Time`) AS year,
        Value
    FROM radiations
"""
)
results.persist()
results_pd = results.toPandas()

results_pd = results_pd.dropna()
results_pd["year"] = results_pd["year"].astype("int64")
results_pd["Value"] = results_pd["Value"].astype("float64")

results_pd = results_pd[(results_pd["year"] <= 2020)]

fig = plt.subplots(figsize=(10, 7))

sns.barplot(x="year", y="Value", data=results_pd, color="orange")
plt.xlabel("Année")
plt.ylabel("Mesures (cpm)")
plt.title("Radiations mesurées en fonction de l'année")
plt.savefig("times.png", format="png")

print("Comptage du nombre de radiations mesurées par année...")

df = df.withColumn("year", substring("Captured Time", 1, 4))
df = df.filter((df.year >= 1900) & (df.year <= 2020))
df.groupBy("year").count().orderBy("count").show()
