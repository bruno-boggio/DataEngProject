# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import count, avg, max, min, sum, round, countDistinct


# COMMAND ----------

# Load the players table
players_df = spark.read.table("tibia_lakehouse_silver.players")

# Aggregation 1: Total and average experience (day, week, month) per vocation
experience_stats_df = players_df.groupBy("vocation") \
    .agg(
        sum("exp_day").alias("total_exp_day"),
        avg("exp_day").alias("avg_exp_day"),
        sum("exp_week").alias("total_exp_week"),
        avg("exp_week").alias("avg_exp_week"),
        sum("exp_month").alias("total_exp_month"),
        avg("exp_month").alias("avg_exp_month")
    ) \
    .orderBy("vocation")

# Aggregation 2: Max, min, and average level per vocation
level_stats_df = players_df.groupBy("vocation") \
    .agg(
        max("level").alias("max_level"),
        min("level").alias("min_level"),
        round(avg("level"), 2).alias("avg_level")
    ) \
    .orderBy("vocation")

# Aggregation 3: Total and average skills (distance, club, axe, sword) per vocation
skills_stats_df = players_df.groupBy("vocation") \
    .agg(
        sum("distance_fighting").alias("total_distance_fighting"),
        avg("distance_fighting").alias("avg_distance_fighting"),
        sum("club_fighting").alias("total_club_fighting"),
        avg("club_fighting").alias("avg_club_fighting"),
        sum("axe_fighting").alias("total_axe_fighting"),
        avg("axe_fighting").alias("avg_axe_fighting"),
        sum("sword_fighting").alias("total_sword_fighting"),
        avg("sword_fighting").alias("avg_sword_fighting")
    ) \
    .orderBy("vocation")

# Aggregation 4: Max, min, and average magic level per vocation
magic_level_stats_df = players_df.groupBy("vocation") \
    .agg(
        max("magic_level").alias("max_magic_level"),
        min("magic_level").alias("min_magic_level"),
        round(avg("magic_level"), 2).alias("avg_magic_level")
    ) \
    .orderBy("vocation")

# Save each aggregation to the 'gold' layer
experience_stats_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.experience_stats")
level_stats_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.level_stats")
skills_stats_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.skills_stats")
magic_level_stats_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.magic_level_stats")

# Print confirmation messages
print("Aggregated tables created successfully!")



# COMMAND ----------

# Load the tables
accounts_df = spark.read.table("tibia_lakehouse_silver.accounts")
players_df = spark.read.table("tibia_lakehouse_silver.players")

# Perform the join between the tables
accounts_players_df = accounts_df.join(players_df, accounts_df.account_id == players_df.account_id, "inner") \
    .select(
        accounts_df.account_id, 
        accounts_df.email,
        players_df.player_id,
        players_df.char_name,
        players_df.level,
        players_df.vocation,
        players_df.gold_amount
    )

# Show the resulting DataFrame
accounts_players_df.show()

# Write the resulting DataFrame to the 'gold' layer, overwriting the existing table
accounts_players_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.accounts_players")

# Print confirmation message
print(f"table: accounts_players created")


# COMMAND ----------

# Calculate total gold and average level per vocation
aggregated_stats_df = accounts_players_df.groupBy("vocation") \
    .agg(
        sum("gold_amount").alias("total_gold"),
        ceil(round(avg("level"), 2)).alias("average_level_rounded")
    ) \
    .orderBy("vocation")

# Show the aggregated DataFrame
aggregated_stats_df.show()

# Write the aggregated DataFrame to the 'gold' layer
aggregated_stats_df.write.format('delta').option("mergeSchema","true").mode('overwrite').saveAsTable("tibia_lakehouse_gold.vocation_stats")

# Print confirmation message
print(f"table: vocation_stats created with total gold and rounded average level per vocation")

# COMMAND ----------

# Load the achievements table
achievements_df = spark.read.table("tibia_lakehouse_silver.achievements")

# Perform the join between the accounts_players_df and achievements tables
accounts_players_achievements_df = accounts_players_df.join(achievements_df, accounts_players_df.player_id == achievements_df.player_id, "left_outer") \
    .select(
        accounts_players_df.account_id,
        accounts_players_df.email,
        accounts_players_df.player_id,
        accounts_players_df.char_name,
        accounts_players_df.level,
        accounts_players_df.vocation,
        accounts_players_df.gold_amount,
        achievements_df.achievement_id,
        achievements_df.achievement_name,
        achievements_df.date_achieved
    )

# Show the resulting DataFrame
accounts_players_achievements_df.show()

# Write the resulting DataFrame to the 'gold' layer, overwriting the existing table
accounts_players_achievements_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.accounts_players_achievements")

# Print confirmation message
print(f"table: accounts_players_achievements created")


# COMMAND ----------

# Load the tables
players_df = spark.read.table("tibia_lakehouse_silver.players")
items_df = spark.read.table("tibia_lakehouse_silver.items")

# Perform the join between the tables
players_items_df = players_df.join(
    items_df, players_df.player_id == items_df.player_id, "inner"
) \
    .select(
        players_df.player_id, 
        players_df.char_name,
        players_df.level,
        players_df.vocation,
        items_df.item_id,
        items_df.item_name,
        items_df.item_type,
        items_df.date_received
    )

# Show the resulting DataFrame
players_items_df.show()

# Write the resulting DataFrame to the 'gold' layer, overwriting the existing table
players_items_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.players_items")

# Print confirmation message
print(f"table: players_items created")


# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, avg, max, round

# Load the players_items table
players_items_df = spark.read.table("tibia_lakehouse_gold.players_items")

# Aggregation 1: Total items and unique items per vocation
items_per_vocation_df = players_items_df.groupBy("vocation") \
    .agg(
        count("item_id").alias("total_items"),
        countDistinct("item_id").alias("unique_items")
    ) \
    .orderBy("vocation")

# Aggregation 2: Most and least items received by a single player
items_per_player_df = players_items_df.groupBy("player_id", "char_name") \
    .agg(
        count("item_id").alias("total_items_received")
    ) \
    .orderBy("total_items_received", ascending=False)

# Aggregation 3: Average and maximum items received per level
items_per_level_df = players_items_df.groupBy("level") \
    .agg(
        avg("item_id").alias("avg_items_per_level"),
        count("item_id").alias("total_items_per_level"),
        max("item_id").alias("max_items_per_level"),
    ) \
    .orderBy("level")

# Aggregation 4: Total items received by item type
items_per_type_df = players_items_df.groupBy("item_type") \
    .agg(
        count("item_id").alias("total_items"),
        countDistinct("player_id").alias("players_with_item_type"),
        round(avg("level"), 2).alias("avg_level_of_players_with_item")
    ) \
    .orderBy("item_type")

# Aggregation 5: Most common items (top 10)
top_items_df = players_items_df.groupBy("item_name") \
    .agg(
        count("item_id").alias("total_occurrences")
    ) \
    .orderBy("total_occurrences", ascending=False) \
    .limit(10)

# Save the aggregated DataFrames to the 'gold' layer
items_per_vocation_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.items_per_vocation")
items_per_player_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.items_per_player")
items_per_level_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.items_per_level")
items_per_type_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.items_per_type")
top_items_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.top_items")

# Print confirmation messages
print("Aggregated tables created successfully:")
print("- tibia_lakehouse_gold.items_per_vocation")
print("- tibia_lakehouse_gold.items_per_player")
print("- tibia_lakehouse_gold.items_per_level")
print("- tibia_lakehouse_gold.items_per_type")
print("- tibia_lakehouse_gold.top_items")

# COMMAND ----------

# Load the tables
players_df = spark.read.table("tibia_lakehouse_silver.players")
guilds_df = spark.read.table("tibia_lakehouse_silver.guilds")

# Fill null values in guilds DataFrame for guild_leader
corrected_guilds_df = guilds_df.fillna({"guild_name": "No Guild", "guild_leader": "No Leader"})

# Perform the join between the tables
players_guilds_df = players_df.join(
    corrected_guilds_df, players_df.guild_id == corrected_guilds_df.guild_id, "left"
) \
    .select(
        players_df.player_id, 
        players_df.char_name,
        players_df.level,
        players_df.vocation,
        corrected_guilds_df.guild_id,
        corrected_guilds_df.guild_name,
        corrected_guilds_df.guild_leader
    )

players_guilds_df = players_guilds_df.drop('guild_leader')

# Show the resulting DataFrame
players_guilds_df.show()

# Write the resulting DataFrame to the 'gold' layer, overwriting the existing table
players_guilds_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.players_guilds")

# Print confirmation message
print(f"table: players_guilds created")


# COMMAND ----------

# Load the tables
players_df = spark.read.table("tibia_lakehouse_silver.players")
transactions_df = spark.read.table("tibia_lakehouse_silver.transactions")

# Perform the join between the tables
players_transactions_df = players_df.join(
    transactions_df, (players_df.player_id == transactions_df.sender_player_id) | 
                     (players_df.player_id == transactions_df.receiver_player_id), "inner"
) \
    .select(
        players_df.player_id,
        players_df.char_name,
        players_df.level,
        players_df.vocation,
        transactions_df.transaction_id,
        transactions_df.transaction_type,
        transactions_df.amount,
        transactions_df.transaction_date,
        transactions_df.sender_player_id,
        transactions_df.receiver_player_id
    )

# Show the resulting DataFrame
players_transactions_df.show()

# Write the resulting DataFrame to the 'gold' layer, overwriting the existing table
players_transactions_df.write.format('delta').mode('overwrite').saveAsTable("tibia_lakehouse_gold.players_transactions")

# Print confirmation message
print(f"table: players_transactions created")

