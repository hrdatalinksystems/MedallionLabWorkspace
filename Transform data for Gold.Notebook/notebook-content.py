# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "be77941c-5334-4b9c-89ee-abbd15b311bf",
# META       "default_lakehouse_name": "Sales",
# META       "default_lakehouse_workspace_id": "cdb8545b-00f3-45dc-98a0-ad5cdeeb576d",
# META       "known_lakehouses": [
# META         {
# META           "id": "be77941c-5334-4b9c-89ee-abbd15b311bf"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Load data to the dataframe as a starting point to create the gold layer

df = spark.read.table("Sales.sales_silver")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *

DeltaTable.createIfNotExists(spark) \
    .tableName("Sales.dimdate_gold") \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Day", IntegerType()) \
    .addColumn("Month", IntegerType()) \
    .addColumn("Year", IntegerType()) \
    .addColumn("mmmyyyy", StringType()) \
    .addColumn("yyyymm", StringType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, dayofmonth, month, year, date_format

dfdimDate_gold = df.dropDuplicates(["OrderDate"]).select(
    col("OrderDate"), \
    dayofmonth("OrderDate").alias("Day"), \
    month("OrderDate").alias("Month"), \
    year("OrderDate").alias("Year"), \
    date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"), \
    date_format(col("OrderDate"), "yyyyMM").alias("yyyymm"), \
    ).orderBy("OrderDate")

display(dfdimDate_gold)
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "Tables/dimdate_gold")

dfUpdates = dfdimDate_gold

deltaTable.alias('gold') \
    .merge(
        dfUpdates.alias('updates'),
        "gold.OrderDate = updates.OrderDate"
    ) \
    .whenMatchedUpdate(set =
        {

        }
    ) \
    .whenNotMatchedInsert(values =
        {
            "OrderDate": "updates.OrderDate",
            "Day": "updates.Day",
            "Month": "updates.Month",
            "Year": "updates.Year",
            "mmmyyyy": "updates.mmmyyyy",
            "yyyymm": "updates.yyyymm"
        }
    ) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StringType, LongType
from delta.tables import *

DeltaTable.createIfNotExists(spark) \
    .tableName("Sales.dimcustomer_gold") \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email",  StringType()) \
    .addColumn("First", StringType()) \
    .addColumn("Last", StringType()) \
    .addColumn("CustomerID", LongType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, split

dfdimCustomer_silver = df.dropDuplicates(["CustomerName", "Email"]).select(col("CustomerName"),col("Email")) \
    .withColumn("First",split(col("CustomerName"), " ").getItem(0)) \
    .withColumn("Last",split(col("CustomerName"), " ").getItem(1))

#Below is alternate code to achieve same Datafram
"""dfdimCustomer_silver = df.dropDuplicates(["CustomerName", "Email"]).select(
        col("CustomerName"),
        col("Email"),
        split(col("CustomerName"), " ").getItem(0).alias("First"),
        split(col("CustomerName"), " ").getItem(1).alias("Last")
    )"""

display(dfdimCustomer_silver.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import coalesce, max, col, lit, monotonically_increasing_id

dfdimCustomer_temp = spark.read.table("Sales.dimcustomer_gold")

MAXCustomerID = dfdimCustomer_temp.select(coalesce(max(col("CustomerName")), lit(0)).alias("MAXCustomerID")).first()[0]

dfdimCustomer_gold = dfdimCustomer_silver.join( \
                    dfdimCustomer_temp,(dfdimCustomer_silver.CustomerName == dfdimCustomer_temp.CustomerName) & \
                    (dfdimCustomer_silver.Email == dfdimCustomer_temp.Email), \
                    'left_anti')

dfdimCustomer_gold = dfdimCustomer_gold.withColumn("CustomerID", monotonically_increasing_id() + MAXCustomerID + 1)

display(dfdimCustomer_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "Tables/dimcustomer_gold")

dfUpdates = dfdimCustomer_gold

deltaTable.alias('gold') \
    .merge(
        dfUpdates.alias('updates'),
        'gold.CustomerName = updates.CustomerName and gold.Email = updates.Email'
    ) \
    .whenMatchedUpdate(set =
        {

        }
    ) \
    .whenNotMatchedInsert(values =
        {
            "CustomerName": 'updates.CustomerName',
            "Email": "updates.Email",
            "First": "updates.First",
            "Last": "updates.Last",
            "CustomerID": "updates.CustomerID"
        }
    ) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *
    
DeltaTable.createIfNotExists(spark) \
    .tableName("sales.dimproduct_gold") \
    .addColumn("ItemName", StringType()) \
    .addColumn("ItemID", LongType()) \
    .addColumn("ItemInfo", StringType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, split, lit, when
    
# Create product_silver dataframe
    
dfdimProduct_silver = df.dropDuplicates(["Item"]).select(col("Item")) \
    .withColumn("ItemName",split(col("Item"), ", ").getItem(0)) \
    .withColumn("ItemInfo",when((split(col("Item"), ", ").getItem(1).isNull() | (split(col("Item"), ", ").getItem(1)=="")),lit("")).otherwise(split(col("Item"), ", ").getItem(1))) 
    
# Display the first 10 rows of the dataframe to preview your data

display(dfdimProduct_silver.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id, col, lit, max, coalesce
    
#dfdimProduct_temp = dfdimProduct_silver
dfdimProduct_temp = spark.read.table("Sales.dimProduct_gold")
    
MAXProductID = dfdimProduct_temp.select(coalesce(max(col("ItemID")),lit(0)).alias("MAXItemID")).first()[0]
    
dfdimProduct_gold = dfdimProduct_silver.join(dfdimProduct_temp,(dfdimProduct_silver.ItemName == dfdimProduct_temp.ItemName) & (dfdimProduct_silver.ItemInfo == dfdimProduct_temp.ItemInfo), "left_anti")
    
dfdimProduct_gold = dfdimProduct_gold.withColumn("ItemID",monotonically_increasing_id() + MAXProductID + 1)
    
# Display the first 10 rows of the dataframe to preview your data

display(dfdimProduct_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/dimproduct_gold')
            
dfUpdates = dfdimProduct_gold
            
deltaTable.alias('gold') \
  .merge(
        dfUpdates.alias('updates'),
        'gold.ItemName = updates.ItemName AND gold.ItemInfo = updates.ItemInfo'
        ) \
        .whenMatchedUpdate(set =
        {
               
        }
        ) \
        .whenNotMatchedInsert(values =
         {
          "ItemName": "updates.ItemName",
          "ItemInfo": "updates.ItemInfo",
          "ItemID": "updates.ItemID"
          }
          ) \
          .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import LongType, DateType, IntegerType, FloatType
from delta.tables import DeltaTable

DeltaTable.createIfNotExists(spark) \
    .tableName("Sales.factsales_gold") \
    .addColumn("CustomerID", LongType()) \
    .addColumn("ItemID", LongType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import split, col, when, lit

dfdimCustomer_temp = spark.read.table("dimcustomer_gold")
dfdimProduct_temp = spark.read.table("dimproduct_gold")
dfdimDate_temp = spark.read.table("dimdate_gold")

df = df.withColumn("ItemName", split(col("Item"), ", ").getItem(0)) \
        .withColumn("ItemInfo", \
            when( (split(col("Item"), ", ").getItem(1).isNull()) \
                | (split(col("Item"), ", ").getItem(1) == lit("")), lit("") \
                ) \
            .otherwise(split(col("Item"), ", ").getItem(1)) \
        )

"""
dffactSales_gold = df.alias('df1').join(
        dfdimCustomer_temp.alias('df2'),
        (df.CustomerName == dfdimCustomer_temp.CustomerName) & (df.Email == dfdimCustomer_temp.Email),
        "left"
    ).join(
        dfdimProduct_temp.alias('df3'),
        (df.ItemName == dfdimProduct_temp.ItemName) & (df.ItemInfo == dfdimProduct_temp.ItemInfo),
        "left"
    ) \
    .select(
        col("df2.CustomerID"),
        col("df3.ItemID"),
        col("df1.OrderDate"),
        col("df1.Quantity"),
        col("df1.UnitPrice"),
        col("df1.Tax")
    ) \
    .orderBy(
        col("df1.OrderDate"),
        col("df2.CustomerID"),
        col("df3.ItemID")
    )
"""

# Below is easy way compared to above
dffactSales_gold = df.join(
        dfdimCustomer_temp,
        (df.CustomerName == dfdimCustomer_temp.CustomerName) & (df.Email == dfdimCustomer_temp.Email),
        "left"
    ).join(
        dfdimProduct_temp,
        (df.ItemName == dfdimProduct_temp.ItemName) & (df.ItemInfo == dfdimProduct_temp.ItemInfo),
        "left"
    ) \
    .select(
        dfdimCustomer_temp.CustomerID,
        dfdimProduct_temp.ItemID,
        df.OrderDate,
        df.Quantity,
        df.UnitPrice,
        df.Tax
    ) \
    .orderBy(
        df.OrderDate,
        dfdimCustomer_temp.CustomerID,
        dfdimProduct_temp.ItemID
    )

display(dffactSales_gold.head(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, "Tables/factsales_gold")

dfUpdates = dffactSales_gold

deltaTable.alias('gold') \
    .merge(
        dfUpdates.alias('updates'),
        """ gold.OrderDate = updates.OrderDate
            and gold.CustomerID = updates.CustomerID
            and gold.ItemID = updates.ItemID"""
    ) \
    .whenMatchedUpdate( set =
        {

        }
    ) \
    .whenNotMatchedInsert( values =
        {
            "CustomerID": 'updates.CustomerID',
            "ItemID": 'updates.ItemID',
            "OrderDate": 'updates.OrderDate',
            "Quantity": 'updates.Quantity',
            "UnitPrice": 'updates.UnitPrice',
            "Tax": 'updates.Tax',
        }
    ) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
