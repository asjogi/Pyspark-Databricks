# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#COMMENTS -----------CREATING SPARK SESSION OBJECT
spark_sc=SparkSession.Builder().appName('Ex01').master('local[*]').getOrCreate()

# COMMAND ----------READ FILE FROM DATA BRICKS LOCATION SAVED FILE

read_data=spark_sc.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/5000_Sales_Records.csv')

# COMMAND ----------DROPPING FEW UNWANTED COLUMNS FROM DATA

read_filter = read_data.drop('Region', 'Country', 'Order Priority', 'Ship Date', 'Total Profit', 'Total Cost', 'Unit Cost')

# COMMAND ----------RENAMING COLUMN NAMES BY REPLACING SPACES WITH UNDERSCORE AND USING LOWER CASES

df_1 = read_filter.withColumnRenamed('Order Id','order_id') \
.withColumnRenamed('Item Type','item_type') \
.withColumnRenamed('Sales Channel','sales_channel') \
.withColumnRenamed('Order Date','order_date') \
.withColumnRenamed('Units Sold','units_sold') \
.withColumnRenamed('Unit Price','unit_price') \
.withColumnRenamed('Total Revenue','total_revenue')
            

# COMMAND ----------CONVERTING DATA TYPES FOR COLUMNS

df_2 = df_1.withColumn('units_sold',col('units_sold').cast('float')) \
.withColumn('unit_price',col('unit_price').cast('float')) \
.withColumn('total_revenue',col('total_revenue').cast('float')) \
.withColumn('order_date',to_date(col('order_date'), 'M/d/yyyy'))


# COMMAND ----------CREATING TABLE IN DATABRICKS

df_2.repartition(3).write.mode('overwrite').saveAsTable('Table_Sales')