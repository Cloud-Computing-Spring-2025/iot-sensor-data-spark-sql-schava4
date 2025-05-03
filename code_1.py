from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, hour, to_timestamp, when, rank
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# ====================================================================
# Initialize Spark Session
# ====================================================================
spark = SparkSession.builder.appName("IoTSensorDataAnalysis").getOrCreate()

# ====================================================================
# Task 1: Load & Basic Exploration
# ====================================================================
# 1. Load sensor_data.csv (assumes header exists and schema is inferred)
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# 2. Create a temporary view for SQL queries
df.createOrReplaceTempView("sensor_readings")

# 3. Basic Queries
print("------ Task 1: Basic Exploration ------")
print("First 5 rows:")
df.show(5)

print("Total record count:")
record_count = df.count()
print(record_count)

print("Distinct locations:")
df.select("location").distinct().show()

print("Distinct sensor types:")
df.select("sensor_type").distinct().show()

# 4. Write the DataFrame (or any key query result) to CSV
df.write.csv("task1_output.csv", header=True, mode="overwrite")


# ====================================================================
# Task 2: Filtering & Simple Aggregations
# ====================================================================
# 1. Filtering: Mark rows as "in-range" if temperature is between 18 and 30 (inclusive); otherwise "out-of-range"
df_filtered = df.withColumn(
    "range_status",
    when((col("temperature") >= 18) & (col("temperature") <= 30), "in-range").otherwise("out-of-range")
)

print("------ Task 2: Filtering ------")
df_filtered.groupBy("range_status").count().show()

# 2. Aggregations: Group by location, compute average temperature and humidity
agg_df = df.groupBy("location").agg(
    avg("temperature").alias("avg_temperature"),
    avg("humidity").alias("avg_humidity")
)

# Order by average temperature descending to spot the hottest locations
agg_df_sorted = agg_df.orderBy(col("avg_temperature").desc())
print("------ Aggregated Data by Location ------")
agg_df_sorted.show()

# 3. Write the aggregated results to CSV
agg_df_sorted.write.csv("task2_output.csv", header=True, mode="overwrite")


# ====================================================================
# Task 3: Time-Based Analysis
# ====================================================================
# 1. Convert the timestamp column to proper timestamp datatype
df_time = df.withColumn("timestamp_ts", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
df_time.createOrReplaceTempView("sensor_readings_time")

# 2. Extract the hour (0-23) from the timestamp and group by hour to calculate average temperature
hourly_avg = df_time.withColumn("hour_of_day", hour("timestamp_ts")) \
    .groupBy("hour_of_day") \
    .agg(avg("temperature").alias("avg_temp")) \
    .orderBy("hour_of_day")

print("------ Task 3: Average Temperature by Hour ------")
hourly_avg.show()

# Optional: Identify the hour with the highest average temperature
print("Hour with the highest average temperature:")
hottest_hour = hourly_avg.orderBy(col("avg_temp").desc())
hottest_hour.show(1)

# 3. Write the results to CSV
hourly_avg.write.csv("task3_output.csv", header=True, mode="overwrite")


# ====================================================================
# Task 4: Window Function - Rank Sensors by Average Temperature
# ====================================================================
# 1. Compute each sensor’s average temperature over all readings
sensor_avg = df.groupBy("sensor_id").agg(avg("temperature").alias("avg_temp"))

# 2. Define a window specification ordering by average temperature descending
windowSpec = Window.orderBy(col("avg_temp").desc())

# 3. Apply a ranking function over the window
sensor_ranked = sensor_avg.withColumn("rank_temp", rank().over(windowSpec))

# 4. Show the top 5 sensors
top_sensors = sensor_ranked.orderBy("rank_temp").limit(5)
print("------ Task 4: Top 5 Sensors by Average Temperature ------")
top_sensors.show()

# 5. Write the ranking results to CSV
top_sensors.write.csv("task4_output.csv", header=True, mode="overwrite")


# ====================================================================
# Task 5: Pivot & Interpretation
# ====================================================================
# 1. Ensure the timestamp is properly formatted and extract the hour of day
df_pivot = df.withColumn("timestamp_ts", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("hour_of_day", hour("timestamp_ts"))

# 2. Create a pivot table with location as rows, hours (0-23) as columns and average temperature as cell values
pivot_df = df_pivot.groupBy("location") \
    .pivot("hour_of_day", list(range(0, 24))) \
    .agg(avg("temperature").alias("avg_temperature"))

print("------ Task 5: Pivot Table (Location x Hour) ------")
pivot_df.show(truncate=False)

# 3. Determine which (location, hour) has the highest average temperature.
# Here we first create a long form of the pivot information:
pivot_long = df_pivot.groupBy("location", "hour_of_day") \
    .agg(avg("temperature").alias("avg_temp"))
print("------ (Location, Hour) with Highest Average Temperature ------")
max_temp_entry = pivot_long.orderBy(col("avg_temp").desc()).limit(1)
max_temp_entry.show()

# 4. Write the pivoted DataFrame to CSV
pivot_df.write.csv("task5_output.csv", header=True, mode="overwrite")


# ====================================================================
# Finalize
# ====================================================================
spark.stop()