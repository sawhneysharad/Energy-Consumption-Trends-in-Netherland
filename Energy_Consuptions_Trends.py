import numpy as np
import pandas as pd
import os

import pyspark
from pyspark.sql.functions import lit

#Electricity Data
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_electricity_01012009.csv')
newdf=df.withColumn('year', lit(2009))
newdf.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/lianderelectricity_012009.csv")

df1 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_electricity_01012010.csv')
newdf1=df1.withColumn('year', lit(2010))
newdf1.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/lianderelectricity_012010.csv")


df2 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_electricity_01012011.csv')
newdf2=df2.withColumn('year', lit(2011))
newdf2.write.format('csv').option("header", "true").option("inferSchema", "true") .option("delimiter", ",").save("/FileStore/tables/lianderelectricity_012011.csv")


df3 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_electricity_01012012.csv')
newdf3=df3.withColumn('year', lit(2012))
newdf3.write.format('csv').option("header", "true").option("inferSchema", "true") .option("delimiter", ",").save("/FileStore/tables/lianderelectricity_012012.csv")

df4 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_electricity_01012013.csv')
newdf4=df4.withColumn('year', lit(2013))
newdf4.write.format('csv').option("header", "true") .option("inferSchema", "true") .option("delimiter", ",") .save("/FileStore/tables/lianderelectricity_012013.csv")


df5 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_electricity_01012014.csv')
newdf5=df5.withColumn('year', lit(2014))
newdf5.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/lianderelectricity_012014.csv")


df6 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_electricity_01012015.csv')
newdf6=df6.withColumn('year', lit(2015))
newdf6.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/lianderelectricity_012015.csv")



df7 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_electricity_01012016.csv')
newdf7=df7.withColumn('year', lit(2016))
newdf7.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/lianderelectricity_012016.csv")


df8 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_electricity_01012017.csv')
newdf8=df8.withColumn('year', lit(2017))
newdf8.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/lianderelectricity_012017.csv")


df9 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_electricity_01012018.csv')
newdf9=df9.withColumn('year', lit(2018))
newdf9.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/lianderelectricity_012018.csv")


df10 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_electricity_01012019.csv')
newdf10=df10.withColumn('year', lit(2019))
newdf10.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/lianderelectricity_012019.csv")


alldf = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/lianderelectricity_*.csv')


alldf=alldf.toPandas()

alldf["purchase_area"].fillna("Liander NW", inplace = True)

alldf=sqlContext.createDataFrame(alldf)

alldf.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/cleanedlianderelectricity.csv")


#Gas data

#gas
gasdf = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_gas_01012009.csv')
newgasdf=gasdf.withColumn('year', lit(2009))
newgasdf.write.format('csv').option("header", "true") .option("inferSchema", "true") .option("delimiter", ",") .save("/FileStore/tables/liandergas_012009.csv")

gasdf1 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_gas_01012010.csv')
newgasdf1=gasdf1.withColumn('year', lit(2010))
newgasdf1.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/liandergas_012010.csv")


gasdf2 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_gas_01012010.csv')
newgasdf2=gasdf2.withColumn('year', lit(2011))
newgasdf2.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/liandergas_012010.csv")


gasdf3 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_gas_01012011.csv')
newgasdf3=gasdf3.withColumn('year', lit(2012))
newgasdf3.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/liandergas_012011.csv")



gasdf4 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_gas_01012012.csv')
newgasdf4=gasdf4.withColumn('year', lit(2013))
newgasdf4.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/liandergas_012012.csv")


df5 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_gas_01012013.csv')
newdf5=df5.withColumn('year', lit(2014))
newdf5.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/liandergas_012013.csv")


gasdf6 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_gas_01012014.csv')
newgasdf6=gasdf6.withColumn('year', lit(2015))
newgasdf6.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/liandergas_012014.csv")



gasdf7 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_gas_01012015.csv')
newgasdf7=gasdf7.withColumn('year', lit(2016))
newgasdf7.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/liandergas_012015.csv")


gasdf8 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_gas_01012016.csv')
newgasdf8=gasdf8.withColumn('year', lit(2017))
newgasdf8.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/liandergas_012016.csv")


gasdf9 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_gas_01012017.csv')
newgasdf9=gasdf9.withColumn('year', lit(2018))
newgasdf9.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/liandergas_012017.csv")


gasdf10 = sqlContext.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/FileStore/tables/liander_gas_01012018.csv')
newgasdf10=gasdf10.withColumn('year', lit(2019))
newgasdf10.write.format('csv').option("header", "true").option("inferSchema", "true").option("delimiter", ",").save("/FileStore/tables/liandergas_012018.csv")


allgasdf=allgasdf.toPandas()

allgasdf["purchase_area"].fillna("Liander NW", inplace = True)

allgasdf=sqlContext.createDataFrame(allgasdf)

allgasdf.write.format('csv').option("header", "true") .option("inferSchema", "true") .option("delimiter", ",") .save("/FileStore/tables/cleanedlianderegas.csv")







