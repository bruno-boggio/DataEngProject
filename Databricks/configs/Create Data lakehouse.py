# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create the database for the RAW layer (Bronze)
# MAGIC CREATE DATABASE IF NOT EXISTS tibia_lakehouse_raw;
# MAGIC
# MAGIC -- Create the database for the SILVER layer
# MAGIC CREATE DATABASE IF NOT EXISTS tibia_lakehouse_silver;
# MAGIC
# MAGIC -- Create the database for the GOLD layer
# MAGIC CREATE DATABASE IF NOT EXISTS tibia_lakehouse_gold;
# MAGIC
# MAGIC
