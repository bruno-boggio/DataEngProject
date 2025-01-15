# Databricks notebook source
from pyspark.sql import functions as F


# COMMAND ----------

# MAGIC %md
# MAGIC **Cleaning Table's duplicate Data**

# COMMAND ----------

# Read the raw data from the "accounts" table and perform transformations
accounts_df = (
    spark.read.table("tibia_lakehouse_raw.accounts") # Read the raw data
    .drop("insertion_timestamp") # Drop insertion_timestamp" 
    .dropDuplicates(["email"]) # Remove duplicates based on "email"
    .withColumnRenamed("player_name", "char_name") # Rename "player_name" to "char_name"
    .withColumnRenamed("timestamp", "raw_timestamp") # Rename "timestamp" to "raw_timestamp"
    .withColumn("silver_timestamp", F.current_timestamp()) # Add the current timestamp as "silver_timestamp"
)

# Display the cleaned data
accounts_df.display()

# Save the cleaned data to the silver layer (overwrite the existing table)
accounts_df.write.format("delta").option("mergeSchema",True).mode("overwrite").saveAsTable("tibia_lakehouse_silver.accounts")

# Print a confirmation message once the table has been saved
print("Table accounts has been saved!")


# COMMAND ----------

 # Read the raw data from the "players" table and perform transformations
players_df = (
    spark.read.table("tibia_lakehouse_raw.players")  # Read the raw data
    .drop("insertion_timestamp")  # Drop the unwanted "insertion_timestamp" column
    .dropDuplicates(["player_name"])  # Remove duplicates based on "player_name"
    .withColumnRenamed("player_name", "char_name")  # Rename "player_name" to "char_name"
    .withColumnRenamed("timestamp", "raw_timestamp")  # Rename "timestamp" to "raw_timestamp"
    .withColumn("silver_timestamp", F.current_timestamp())  # Add the current timestamp as "silver_timestamp"
)

# Display the cleaned and transformed data
players_df.display()

# Save the transformed data to the silver layer (tibia_lakehouse_silver)
players_df.write.format("delta").mode("overwrite").saveAsTable("tibia_lakehouse_silver.players")

# Print a confirmation message
print(f"Table players has been saved!")


# COMMAND ----------

# Read the raw data from the "achievements" table and perform transformations
achievements_df = (
    spark.read.table("tibia_lakehouse_raw.achievements")  # Read the raw data
    .drop("insertion_timestamp")  # Drop the unwanted "insertion_timestamp" column
    .withColumnRenamed("timestamp", "raw_timestamp")  # Rename "timestamp" to "raw_timestamp"
    .withColumn("silver_timestamp", F.current_timestamp())  # Add the current timestamp as "silver_timestamp"
)

# Display the cleaned and transformed data
achievements_df.display()

# Save the transformed data to the silver layer (tibia_lakehouse_silver)
achievements_df.write.format("delta").mode("overwrite").saveAsTable("tibia_lakehouse_silver.achievements")

# Print a confirmation message
print(f"Table achievements has been saved!")


# COMMAND ----------

# Read the raw data from the "guilds" table
guilds_df = spark.read.table("tibia_lakehouse_raw.guilds")

# Transformations: Drop the "insertion_timestamp" column, remove duplicates, rename "timestamp", and add "silver_timestamp"
guilds_df = (
    guilds_df
    .drop("insertion_timestamp")  # Drop the unwanted column
    .dropDuplicates(["guild_name"])  # Remove duplicates based on "guild_name"
    .withColumnRenamed("timestamp", "raw_timestamp")  # Rename "timestamp" to "raw_timestamp"
    .withColumn("silver_timestamp", F.current_timestamp())  # Add the current timestamp column
)

# Display the cleaned data
guilds_df.display()

# Save the cleaned and transformed data to the silver layer
guilds_df.write.format("delta").mode("overwrite").saveAsTable("tibia_lakehouse_silver.guilds")

# Print a confirmation message
print(f"Table guilds has been saved!")


# COMMAND ----------

# Read the raw data from the "items" table and perform transformations
items_df = (
    spark.read.table('tibia_lakehouse_raw.items')  # Read the raw data
    .drop('insertion_timestamp')  # Drop the unwanted "insertion_timestamp" column
    .dropDuplicates(['item_name'])  # Remove duplicates based on "item_name"
    .withColumnRenamed('timestamp', 'raw_timestamp')  # Rename "timestamp" to "raw_timestamp"
    .withColumn('silver_timestamp', F.current_timestamp())  # Add the current timestamp as "silver_timestamp"
)

# Display the cleaned and transformed data
items_df.display()

# Save the transformed data to the silver layer (tibia_lakehouse_silver)
items_df.write.format("delta").mode("overwrite").saveAsTable("tibia_lakehouse_silver.items")

# Print a confirmation message
print(f"Table items has been saved!")


# COMMAND ----------

# Read the raw data from the "transactions" table
transactions_df = spark.read.table('tibia_lakehouse_raw.transactions')

# Drop the unwanted "insertion_timestamp" column
transactions_df = (
    transactions_df
    .drop('insertion_timestamp')  # Removing unnecessary column "insertion_timestamp"
    .dropDuplicates(['transaction_id'])  # Removing duplicates based on the "transaction_id" column
    .withColumnRenamed('timestamp', 'raw_timestamp')  # Renaming "timestamp" column to "raw_timestamp"
    .withColumn('silver_timestamp', F.current_timestamp())  # Adding the current timestamp as "silver_timestamp"
)

# Display the cleaned DataFrame
transactions_df.display()

# Save the cleaned DataFrame to the "tibia_lakehouse_silver" layer
transactions_df.write.format("delta").mode("overwrite").saveAsTable("tibia_lakehouse_silver.transactions")

# Print a confirmation message
print(f"Table transactions has been saved!")


# COMMAND ----------

# MAGIC %md
# MAGIC **Creating tables through joins**
