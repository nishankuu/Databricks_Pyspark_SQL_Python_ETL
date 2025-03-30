# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark=SparkSession.builder.appName("IPL Apache Spark Analysis").getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType,StringType,BooleanType, DateType, DecimalType
from pyspark.sql.functions import col,sum,avg, row_number,when,round,count
from pyspark.sql.window import Window

# COMMAND ----------

# Define the schema of ball_by_ball table
ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# COMMAND ----------

# S3 bucket details
s3_bucket = "Give your bucket name here"
s3_key = "File name"

# If IAM role is not attached, configure AWS credentials manually
aws_access_key = "Access Key"
aws_secret_key = "Secret key"

# Set Hadoop configurations for S3 access
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "Access Key")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "Secret Key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

ball_by_ball_df=spark.read.format("csv").schema(ball_by_ball_schema).option("header","true").load("s3://ipl-pyspark-databricks-engineering/Ball_By_Ball.csv")

# COMMAND ----------

ball_by_ball_df.show(5)

# COMMAND ----------

ball_by_ball_df.display()

# COMMAND ----------

# Define the match table schema
match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),  # Changed 'year' to IntegerType() since there's no YearType()
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])
Match_df=spark.read.format("csv").schema(match_schema).option("header","true").load("s3://ipl-pyspark-databricks-engineering/Match.csv")

# COMMAND ----------

Match_df.show(5)

# COMMAND ----------

# Define player table schema
player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

Player_df=spark.read.format("csv").schema(player_schema).option("header","true").load("s3://ipl-pyspark-databricks-engineering/Player.csv")

# COMMAND ----------

# Define player match table schema

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(18, 2), True),  # Adjust precision and scale if needed
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),  # `year` type is not available, so using IntegerType
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

Player_match_df=spark.read.format("csv").schema(player_match_schema).option("header","true").load("s3://ipl-pyspark-databricks-engineering/Player_match.csv")

# COMMAND ----------

# Define team table schema

team_schema = StructType([
    StructField("team_sk", IntegerType(), True),        # team_sk: Unique team key
    StructField("team_id", IntegerType(), True),        # team_id: Unique team identifier
    StructField("team_name", StringType(), True)        # team_name: Name of the team
])
team_df=spark.read.format("csv").schema(team_schema).option("header","true").load("s3://ipl-pyspark-databricks-engineering/Team.csv")

# COMMAND ----------

ball_by_ball_df.display(5)

# COMMAND ----------

# Filetr valid deliveries
ball_by_ball_df=ball_by_ball_df.filter((col('wides')==0) & (col('noballs')==0))

# Aggregation - Total and Average runs scored
total_avg_run=ball_by_ball_df.groupBy("match_id","innings_no").agg(
    sum("runs_scored").alias("Total Runs Scored"),
    round(avg("runs_scored"),2).alias("Average_runs")
)

# COMMAND ----------

total_avg_run.display(5)

# COMMAND ----------

# Window function : Calculate running total of runs in each match for each over
windowSpec = Window.partitionBy("match_id","innings_no").orderBy("over_id")

ball_by_ball_df=ball_by_ball_df.withColumn(
    "running_total_runs",
    sum("runs_scored").over(windowSpec)
)
ball_by_ball_df = ball_by_ball_df.drop("running_yotal_runs") # Dropped a coulm name having incorrect label


# COMMAND ----------

# Conditional Formatting Column
ball_by_ball_df=ball_by_ball_df.withColumn(
    "high_impact",
    when(((col("runs_scored")+col("extra_runs"))>6) | (col("bowler_wicket") == True),True).otherwise(False)
)

# COMMAND ----------

ball_by_ball_df.show(5)

# COMMAND ----------

# Analysis of match table

from pyspark.sql.functions import year,month, dayofmonth,when

# Extracting month ,date,year from the date value
Match_df=Match_df.withColumn("year",year("match_date"))
Match_df=Match_df.withColumn("month",month("match_date"))
Match_df=Match_df.withColumn("day",dayofmonth("match_date"))

# COMMAND ----------

# adding high margin win conditional column

Match_df=Match_df.withColumn(
    "High_margin_win",
    when(col("win_margin")>= 100, "High").
    when((col("win_margin")>= 50) & (col("win_margin")<100),"Medium").
    when(col("win_margin")< 50,"Low")
)

# COMMAND ----------

#Analysis of toss  impact

Match_df= Match_df.withColumn(
    "Toss_match_winner",
    when((col("toss_winner") == col("match_winner")),"Yes").otherwise("No")
)

# COMMAND ----------

Match_df.show(5)

# COMMAND ----------

Player_df.display(5)

# COMMAND ----------

# Player table analysis


from pyspark.sql.functions import lower, regexp_replace

# replacing value in a string
Player_df=Player_df.withColumn(
    "player_name", lower(regexp_replace("player_name","[^a-zA-Z0-9 ]","")))

# Handle missing values in 'batting_hand' and 'bowling_skill' with a default 'unknown'
Player_df = Player_df.na.fill({"batting_hand": "unknown", "bowling_skill": "unknown"})

# Categorizing players based on batting hand
Player_df = Player_df.withColumn(
    "batting_style",
    when(col("batting_hand").contains("left"), "Left-Handed").otherwise("Right-Handed")
)

# Show the modified player DataFrame
Player_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import col, when, current_date, expr

# Add a 'veteran_status' column based on player age
Player_match_df = Player_match_df.withColumn(
    "veteran_status",
    when(col("age_as_on_match") >= 35, "Veteran").otherwise("Non-Veteran")
)

# Dynamic column to calculate years since debut
Player_match_df = Player_match_df.withColumn(
    "years_since_debut",
    (year(current_date()) - col("season_year"))
)

# Show the enriched DataFrame
Player_match_df.display(10)

# COMMAND ----------

#creating temporary views for Spark Queries from above generated dataframes

ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
Match_df.createOrReplaceTempView("match")
Player_df.createOrReplaceTempView("player")
Player_match_df.createOrReplaceTempView("player_match")
team_df.createOrReplaceTempView("team")

# COMMAND ----------

# finding top scoring batsmen in each season
# basic SQL knowledge would help to query required data in same notebook

top_scoring_batsmen=spark.sql("""
Select p.player_id,pp.player_name, sum(runs_scored) as Total_runs
FROM player_match p
join ball_by_ball b
ON  p.match_id = b.match_id 
Join player pp
ON p.player_id=pp.player_id
GROUP BY   p.player_id,pp.player_name
ORDER BY   sum(runs_scored) Desc                      
""")

top_scoring_batsmen.display(5)


#We can do more analysis depending on business use case

# COMMAND ----------

# Creating temporary data frame for python analysis

runs_per_over = ball_by_ball_df.groupBy("over_id").agg(sum("runs_scored").alias("Total Runs")).orderBy("over_id")
runs_per_over.display(2)

# Convert Spark DataFrame to Pandas for visualization
runs_per_over_pd = runs_per_over.toPandas()

# Plotting the runs per over
plt.figure(figsize=(12, 6))
plt.plot(runs_per_over_pd["over_id"], runs_per_over_pd["Total Runs"], marker='o', linestyle='-', color='b')
plt.xlabel("Over Number")
plt.ylabel("Total Runs Scored")
plt.title("Total Runs Scored Per Over in IPL Matches")
plt.xticks(range(1, 21))  # Assuming a standard T20 match
plt.grid(True)
plt.show()


# When we need to replace above line and want bar graph 

# Assuming runs_per_over_pd is the Pandas DataFrame with 'over_id' and 'Total Runs'
plt.figure(figsize=(12, 6))
plt.bar(runs_per_over_pd["over_id"], runs_per_over_pd["Total Runs"], color='skyblue')

# Labels and Title
plt.xlabel("Over Number")
plt.ylabel("Total Runs Scored")
plt.title("Total Runs Scored Per Over in IPL Matches")
plt.xticks(range(1, 21))  # Assuming a standard T20 match

# Show the bar chart
plt.show()

# COMMAND ----------

#Using Seaborn to generate visualization in python

import seaborn as sns

# Aggregate Total Runs and Balls Bowled Per Over
economy_df = ball_by_ball_df.groupBy("over_id").agg(
    sum("runs_scored").alias("Total Runs Conceded"),
    count("ball_id").alias("Total Balls Bowled")
)

# Compute Economy Rate
economy_df = economy_df.withColumn("Economy Rate", round((economy_df["Total Runs Conceded"] / economy_df["Total Balls Bowled"]) * 6,2))

# Checking data frame 
economy_df.display()

# Convert to Pandas for Seaborn
economy_pd = economy_df.toPandas()

#  Seaborn Visualization
plt.figure(figsize=(12, 6))
sns.barplot(x="over_id", y="Economy Rate", data=economy_pd, palette="coolwarm")

# Labels and Title
plt.xlabel("Over Number")
plt.ylabel("Economy Rate")
plt.title("Bowling Economy Rate Across Overs")
plt.xticks(range(1, 21))  # Assuming a T20 match

# Show the Plot
plt.show()