df = spark.table("order")
df.where("order_date='2022-01-01'").count()
df.where("order_month='2022-01'").count()

df_dim = spark.read.csv("year_month.csv", header=True, inferSchema=True)
df_dim.show(5)
df_dim.createOrReplaceTempView("year_month")

spark.sql("SELECT year, month, SUM(price) as sales FROM order o JOIN year_month ym ON ym.year_month = o.order_month WHERE year = 2022 and month = 1 GROUP BY 1, 2").count()

spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
spark.sql("SELECT year, month, SUM(price) as sales FROM order o JOIN year_month ym ON ym.year_month = o.order_month WHERE year = 2022 and month = 1 GROUP BY 1, 2").count()
