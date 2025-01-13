# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Path to the Data Lake mount
base_path = "/mnt/tibiadataeng/landing-zone/parquet_tb_files/"
print(f'Base path: {base_path}')

# List all directories (tables) inside the folder
tabela_paths = dbutils.fs.ls(base_path)

table_names = [tabela_path.name.strip("/") for tabela_path in tabela_paths]
print(f'Table names: {table_names}')


# COMMAND ----------

# Defining the schema
schema = 'dbo'

# For each directory (table) inside the folder
for tabela_path in tabela_paths:
    if tabela_path.isDir():
        # Table name (directory)
        table_name = tabela_path.name.strip("/")  # Removes any trailing slash, if present
        print(f"Processing table {table_name}...")

        # Path to the Parquet file with the correct name
        parquet_path = f"{base_path}{table_name}/{schema}.{table_name}.parquet"
        print(f'Parquet path: {parquet_path}')

        # Read the Parquet file
        df = spark.read.parquet(parquet_path)
        print(f"Reading {df.count()} records from table {table_name}...")

        # Add the current timestamp column
        df = df.withColumn("timestamp", current_timestamp())

        # # Create the Delta table in the database
        df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"tibia_lakehouse_raw.{table_name}")
        print(f"Table {table_name} successfully created in the tibia_lakehouse_raw database!")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history tibia_lakehouse_raw.accounts

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tibia_lakehouse_raw.players
