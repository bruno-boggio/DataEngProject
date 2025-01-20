# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Defining widgets parameters
# dbutils.widgets.text("base_path", "/mnt/tibiadataeng/landing-zone/parquet_tb_files/")
# dbutils.widgets.text("schema", "dbo")


# COMMAND ----------

# Getting widgets values
base_path = dbutils.widgets.get("base_path")
schema = dbutils.widgets.get("schema")
print(f'Base path: {base_path}')
print(f'Schema: {schema}')


# COMMAND ----------

# List directory tables
try:
    table_paths = dbutils.fs.ls(base_path)
    table_names = [table_path.name.strip("/") for table_path in table_paths if table_path.isDir()]
    print(f'Table names: {table_names}')
except Exception as e:
    print(f"Erro ao listar os diretórios no path {base_path}: {e}")
    raise e

# COMMAND ----------

# Process each table
for table_path in table_paths:
    if table_path.isDir():
        table_name = table_path.name.strip("/")  # Table's name (diretório)
        print(f"Processing table {table_name}...")

        # Path parquet file
        parquet_path = f"{base_path}{table_name}/{schema}.{table_name}.parquet"
        print(f'Parquet path: {parquet_path}')

        # Read parquet file
        df = spark.read.parquet(parquet_path)
        print(f"Reading {df.count()} records from table {table_name}...")

        # Add timestamp column
        df = df.withColumn("timestamp", current_timestamp())

        # Create delta table in the raw database
        df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(f"tibia_lakehouse_raw.{table_name}")
        print(f"Table {table_name} successfully created in the tibia_lakehouse_raw database!")


# COMMAND ----------

# %sql
# describe history tibia_lakehouse_raw.accounts
