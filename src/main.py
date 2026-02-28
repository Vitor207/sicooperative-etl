import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

from pyspark.sql import SparkSession
from config import JDBC_URL, JDBC_PROPERTIES

spark = (
    SparkSession.builder
    .appName("movimento-flat-etl")
    .master("local[*]")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

# Teste simples lendo uma tabela
df = spark.read.jdbc(
    url=JDBC_URL,
    table="cartao",
    properties=JDBC_PROPERTIES
)

df.show()

spark.stop()