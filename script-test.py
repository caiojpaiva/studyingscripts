import os
import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.functions import sum

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

file_location = "gs://br-apps-dolphin-ddp-dev-supply/CADGOLD_000000000000.csv"
file_type = "csv"
infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

df_teste_1 = spark.read.format(file_type)\
    .option("inferschema", infer_schema)\
    .option("header", first_row_is_header)\
    .option("sep", delimiter)\
    .load(file_location) 

df_teste_filtrado = df_teste_1.filter((df_teste_1.COD_LOJA_GOLD == '355') & (col("QTD_BACKUP_ESTQ").isNotNull())) 

df_teste_filtrado.printSchema()

df_teste_filtrado.show()


df_teste_filtrado.write.parquet("gs://br-apps-dolphin-ddp-dev-supply/dados-teste/CADGOLD.filtrado.parquet")
df_teste_filtrado.write.option("header", "true")\
                       .option("delimiter", ";")\
                       .csv("gs://br-apps-dolphin-ddp-dev-supply/dados-teste/CADGOLD.filtrado.csv")
                       
                          
    
df_teste_filtrado1= df_teste_filtrado.withColumn("QTD_BACKUP_ESTQ", df_teste_filtrado.QTD_BACKUP_ESTQ.cast("Integer"))

df_teste_filtrado2 = df_teste_filtrado1.groupBy("NOME_ORIGEM")\
                                       .agg(sum("QTD_BACKUP_ESTQ"))\
                                       .withColumnRenamed("sum(QTD_BACKUP_ESTQ)", "Total_QTD_BACKUP_ESTQ")\
                                       .show(truncate=False)