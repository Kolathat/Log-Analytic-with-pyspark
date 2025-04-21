from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, substring
import matplotlib.pyplot as plt
import pandas as pd

# create Sparksession
spark = SparkSession.builder \
    .appName("Log Analytics") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

log_file = "apache_logs.txt"
logs_df = spark.read.text(log_file)

logs_df = logs_df.withColumn('ip', regexp_extract('value', r'^(\S+)', 1))
logs_df = logs_df.withColumn('timestamp', regexp_extract('value', r'\[(.*?)\]', 1))
logs_df = logs_df.withColumn('method', regexp_extract('value', r'\"(\S+)', 1))
logs_df = logs_df.withColumn('endpoint', regexp_extract('value', r'\"(?:\S+)\s(\S+)', 1))
logs_df = logs_df.withColumn('status', regexp_extract('value', r'\s(\d{3})\s', 1))

status_counts = logs_df.groupBy('status').count().orderBy(col('count').desc())
status_pd = status_counts.toPandas()
print("\nStatus Code Summary:")
print(status_pd)

top_ips = logs_df.groupBy('ip').count().orderBy(col('count').desc()).limit(10)
top_ips_pd = top_ips.toPandas()
print("\nTop 10 IP addresses:")
print(top_ips_pd)

top_endpoints = logs_df.groupBy('endpoint').count().orderBy(col('count').desc()).limit(10)
top_endpoints_pd = top_endpoints.toPandas()
print("\nTop 10 Endpoints:")
print(top_endpoints_pd)

logs_df = logs_df.withColumn('date', substring('timestamp', 1, 11))
traffic_by_date = logs_df.groupBy('date').count().orderBy('date')
traffic_pd = traffic_by_date.toPandas()

# Visualization
plt.figure(figsize=(6,4))
plt.bar(status_pd['status'], status_pd['count'])
plt.xlabel('Status Code')
plt.ylabel('Count')
plt.title('HTTP Status Code Frequency')
plt.tight_layout()
plt.show()

plt.figure(figsize=(10,4))
plt.plot(traffic_pd['date'], traffic_pd['count'], marker='o')
plt.xlabel('Date')
plt.ylabel('Requests')
plt.title('Traffic by Date')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

status_pd.to_csv('status_summary.csv', index=False)
top_ips_pd.to_csv('top_ips.csv', index=False)
top_endpoints_pd.to_csv('top_endpoints.csv', index=False)
traffic_pd.to_csv('traffic_by_date.csv', index=False)

spark.stop()
