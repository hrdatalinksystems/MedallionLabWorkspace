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

 from pyspark.sql.types import *
    
 # Create the schema for the table
 orderSchema = StructType([
     StructField("SalesOrderNumber", StringType()),
     StructField("SalesOrderLineNumber", IntegerType()),
     StructField("OrderDate", DateType()),
     StructField("CustomerName", StringType()),
     StructField("Email", StringType()),
     StructField("Item", StringType()),
     StructField("Quantity", IntegerType()),
     StructField("UnitPrice", FloatType()),
     StructField("Tax", FloatType())
     ])
    
 # Import all files from bronze folder of lakehouse
 df = spark.read.format("csv").option("header", "true").schema(orderSchema).load("Files/bronze/*.csv")
    
 # Display the first 10 rows of the dataframe to preview your data
 display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name
    
 # Add columns IsFlagged, CreatedTS and ModifiedTS
 df = df.withColumn("FileName", input_file_name()) \
     .withColumn("IsFlagged", when(col("OrderDate") < '2019-08-01',True).otherwise(False)) \
     .withColumn("CreatedTS", current_timestamp()).withColumn("ModifiedTS", current_timestamp())
    
 # Update CustomerName to "Unknown" if CustomerName null or empty
 df = df.withColumn("CustomerName", when((col("CustomerName").isNull() | (col("CustomerName")=="")),lit("Unknown")).otherwise(col("CustomerName")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 # Define the schema for the sales_silver table
    
 from pyspark.sql.types import *
 from delta.tables import *
    
 DeltaTable.createIfNotExists(spark) \
     .tableName("sales.sales_silver") \
     .addColumn("SalesOrderNumber", StringType()) \
     .addColumn("SalesOrderLineNumber", IntegerType()) \
     .addColumn("OrderDate", DateType()) \
     .addColumn("CustomerName", StringType()) \
     .addColumn("Email", StringType()) \
     .addColumn("Item", StringType()) \
     .addColumn("Quantity", IntegerType()) \
     .addColumn("UnitPrice", FloatType()) \
     .addColumn("Tax", FloatType()) \
     .addColumn("FileName", StringType()) \
     .addColumn("IsFlagged", BooleanType()) \
     .addColumn("CreatedTS", DateType()) \
     .addColumn("ModifiedTS", DateType()) \
     .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update existing records and insert new ones based on a condition defined by the columns SalesOrderNumber, OrderDate, CustomerName, and Item.

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/sales_silver')

dfUpdates = df

deltaTable.alias('silver') \
    .merge(
        df.alias('updates'),
        'silver.SalesOrderNumber = updates.SalesOrderNumber and silver.OrderDate = updates.OrderDate and silver.CustomerName = updates.CustomerName and silver.Item = updates.Item'
    ) \
    .whenMatchedUpdate(set =
    {

    }
    ) \
    .whenNotMatchedInsert(values=
        {
            "SalesOrderNumber": "updates.SalesOrderNumber",
            "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
            "OrderDate": "updates.OrderDate",
            "CustomerName": "updates.CustomerName",
            "Email": "updates.Email",
            "Item": "updates.Item",
            "Quantity": "updates.Quantity",
            "UnitPrice": "updates.UnitPrice",
            "Tax": "updates.Tax",
            "FileName": "updates.FileName",
            "IsFlagged": "updates.IsFlagged",
            "CreatedTS": "updates.CreatedTS",
            "ModifiedTS": "updates.ModifiedTS" 
        }
    ) \
    .execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
