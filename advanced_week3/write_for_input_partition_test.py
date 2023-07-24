import numpy as np
import pandas as pd
import random
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("WriteTesting") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# table: ProductID, OrderID, Product, Price
key_1 = [101] * 1000
key_2 = [201] * 10000000
key_3 = [301] * 5000
key_4 = [401] * 1000000
OrderID = key_1 + key_2 + key_3 + key_4
random.shuffle(OrderID)

Qty_1 = list(np.random.randint(low = 1, high = 100, size = len(key_1)))
Qty_2 = list(np.random.randint(low = 1, high = 200, size = len(key_2)))
Qty_3 = list(np.random.randint(low = 1, high = 1000, size = len(key_3)))
Qty_4 = list(np.random.randint(low = 1, high = 50, size = len(key_4)))
Qty = Qty_1 + Qty_2 + Qty_3 + Qty_4

Sales_1 = list(np.random.randint(low = 10, high = 100, size = len(key_1)))
Sales_2 = list(np.random.randint(low = 50, high = 3400, size = len(key_2)))
Sales_3 = list(np.random.randint(low = 12, high = 2000, size = len(key_3)))
Sales_4 = list(np.random.randint(low = 40, high = 1000, size = len(key_4)))
Sales = Sales_1 + Sales_2 + Sales_3 + Sales_4

Discount = list(np.random.randint(low = 0, high = 2, size = len(OrderID)))

data1 = list(zip(OrderID,Qty,Sales,Discount))
data = pd.DataFrame(data1, columns=['OrderID','Qty','Sales','Discount'])
data['created_at'] = np.random.choice(
    pd.date_range('2021-10-01', '2023-02-28'),
    1000+10000000+5000+1000000
)

df = spark.createDataFrame(data)
df.printSchema()
df.show()
print("Number of Partitions", df.rdd.getNumPartitions())

df.repartition(100).write.format("json").mode("overwrite").save("regular100")
df.repartition(100).write.format("json").mode("overwrite").option("compression", "gzip").save("regular100_gzip")
df.repartition(100).write.format("json").mode("overwrite").option("compression", "bzip2").save("regular100_bzip2")
