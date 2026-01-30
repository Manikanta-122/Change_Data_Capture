# -------------------------
# IMPORT REQUIRED MODULES
# -------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
# -------------------------
# CREATE SPARK SESSION
# -------------------------
# Initialize SparkSession and attach MySQL JDBC driver
spark = SparkSession.builder \
    .appName("PySpark CDC Project") \
    .config(
        "spark.jars",
        r"C:\Users\chint\OneDrive\Desktop\connector\mysql-connector-j-8.0.33\mysql-connector-j-8.0.33.jar"
    ) \
    .getOrCreate()
# -------------------------
# READ FULL LOAD FROM SOURCE DB
# -------------------------
# Load existing data from source MySQL database
full_load = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/db name") \
    .option("dbtable", "table name") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()
# Rename columns to maintain consistency
full_load = full_load \
    .withColumnRenamed("PersonID", "id") \
    .withColumnRenamed("FullName", "fullname") \
    .withColumnRenamed("City", "city")
# -------------------------
# READ UPDATED / CDC DATA
# -------------------------
# Read CDC file containing Insert, Update, Delete records
updated_load = spark.read.csv(
    r"C:\Users\chint\PycharmProjects\PythonProject\officeD",
    header=False,
    inferSchema=True
)
# Rename CDC columns
# status -> I (Insert), U (Update), D (Delete)
updated_load = updated_load \
    .withColumnRenamed("_c0", "status") \
    .withColumnRenamed("_c1", "id") \
    .withColumnRenamed("_c2", "fullname") \
    .withColumnRenamed("_c3", "city")
# -------------------------
# APPLY CDC LOGIC
# -------------------------
# Loop through each CDC record
for row in updated_load.collect():

    # UPDATE operation
    # Update fullname and city where id matches
    if row.status == "U":
        full_load = full_load.withColumn(
            "fullname",
            when(full_load.id == row.id, row.fullname)
            .otherwise(full_load.fullname)
        )
        full_load = full_load.withColumn(
            "city",
            when(full_load.id == row.id, row.city)
            .otherwise(full_load.city)
        )
    # INSERT operation
    # Add new record to full_load
    elif row.status == "I":
        new_df = spark.createDataFrame(
            [(row.id, row.fullname, row.city)],
            ["id", "fullname", "city"]
        )
        full_load = full_load.union(new_df)

    # DELETE operation
    # Remove record with matching id
    elif row.status == "D":
        full_load = full_load.filter(full_load.id != row.id)
# -------------------------
# WRITE FINAL DATA TO TARGET DB
# -------------------------
# Write the final processed data into target MySQL database
full_load.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/db name") \
    .option("dbtable", "table name") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .mode("overwrite") \
    .save()