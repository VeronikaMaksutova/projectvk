from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType
import os
from datetime import datetime, timedelta

def main():
    spark = SparkSession.builder \
        .appName("UserActionsAggregation") \
        .getOrCreate()

    target_date_dt = datetime.today()  
    output_directory = "output"  
    daily_directory = "daily_agg" 
    input_directory = "input"  

    start_date_dt = target_date_dt - timedelta(days=7)

    date_list = [(start_date_dt + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

    dataframes = []
    old_schema = StructType() \
        .add("email",StringType(),True) \
        .add("create_count",IntegerType(), True) \
        .add("read_count",IntegerType(),True) \
        .add("update_count",IntegerType(),True) \
        .add("delete_count",IntegerType(),True)
    new_schema = StructType() \
        .add("email",StringType(),True) \
        .add("action",StringType(), True) \
        .add("dt",TimestampType(),True)
    for date in date_list:
        daily_path = f"{daily_directory}/{date}.csv"
        if os.path.exists(f"{daily_directory}/{date}.csv"):
            df = spark.read.csv(daily_path, header=True, schema=old_schema, inferSchema=False)
            dataframes.append(df)
        else:
            input_path = f"{input_directory}/{date}.csv"
            df = spark.read.csv(input_path, header=False, schema=new_schema, inferSchema=False)
            new_daily = df.groupBy("email").pivot("action", ["CREATE", "READ", "UPDATE", "DELETE"]).count()
            new_daily = new_daily.withColumnRenamed("CREATE", "create_count") \
                                .withColumnRenamed("READ", "read_count") \
                                .withColumnRenamed("UPDATE", "update_count") \
                                .withColumnRenamed("DELETE", "delete_count")
            new_daily = new_daily.na.fill(0)
            new_daily.coalesce(1).toPandas().to_csv(daily_path, header=True, index=False)
            dataframes.append(new_daily)

    if dataframes:
        all_data = dataframes[0]
        for df in dataframes[1:]:
            all_data = all_data.union(df)

        weekly_agg = all_data.groupBy("email").sum("create_count", "read_count", "update_count", "delete_count")
        weekly_agg = weekly_agg.withColumnRenamed("sum(create_count)", "create_count") \
                                .withColumnRenamed("sum(read_count)", "read_count") \
                                .withColumnRenamed("sum(update_count)", "update_count") \
                                .withColumnRenamed("sum(delete_count)", "delete_count")

        output_path = f"{output_directory}/{target_date_dt.strftime("%Y-%m-%d")}.csv"
        weekly_agg.coalesce(1).toPandas().to_csv(output_path, header=True, index=False)

        print(f"Aggregated data has been saved to {output_path}")
    else:
        print("No data to process.")