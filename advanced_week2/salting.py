# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession \
    .builder \
    .appName("Salting") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.sql.adaptive.enabled", False) \
    .enableHiveSupport() \
    .getOrCreate()

# 실행 케이스에 맞춰 아래 case 변수 값을 바꿀 것
# 0: AQE는 disable된 상태에서 salting 사용해서 aggregation 수행 
# 1: AQE가 enable된 상태에서 salting 사용해서 aggregation 수행
# 2: AQE는 disable된 상태에서 salting 없이 aggregation 수행 
# 3: AQE는 enable된 상태에서 salting 없이 aggregation 수행
# 4: AQE는 disable된 상태에서 item_id 100번에 대한 salting을 해서 skewed join 수행
# 5: AQE는 disable된 상태에서 전체 레코드에 salting을 해서 skewed join 수행
# 6: AQE는 enable된 상태에서 salting 없이 skewed join 수행
# 7: AQE는 disable된 상태에서 salting 없이 skewed join 수행

case = 1

if case == 0:
    spark.sql("""
SELECT  item_id, SUM(cnt)
FROM (
  SELECT item_id, salt, COUNT(1) cnt
  FROM (
    SELECT FLOOR(RAND()*200) salt, item_id
    FROM sales
  )
  GROUP BY 1, 2
)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100;""").show(5)

elif case == 1:
    spark.conf.set("spark.sql.adaptive.enabled", True)
    spark.sql("""
SELECT  item_id, SUM(cnt) 
FROM (
  SELECT item_id, salt, COUNT(1) cnt
  FROM (
    SELECT FLOOR(RAND()*200) salt, item_id
    FROM sales
  )
  GROUP BY 1, 2
)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100;""").show(5)

elif case == 2:
    spark.sql("""
SELECT item_id, COUNT(1)
FROM sales
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100;
""").show(5)

elif case == 3:
    spark.conf.set("spark.sql.adaptive.enabled", True)
    spark.sql("""
SELECT item_id, COUNT(1)
FROM sales
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100;""").show(5)

elif case == 4:
    spark.sql("""
SELECT date, sum(quantity * price) AS total_sales
FROM (
  SELECT *, CASE WHEN item_id = 100 THEN FLOOR(RAND()*20) ELSE 1 END AS salt 
  FROM sales
) s
JOIN (
  SELECT *, EXPLODE(ARRAY(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)) AS salt 
  FROM items
  WHERE id = 100

  UNION

  SELECT *, 1 AS salt
  FROM items
  WHERE id <> 100
 
) i ON s.item_id = i.id and s.salt = i.salt
GROUP BY 1
ORDER BY 2 DESC;""").show(5)

elif case == 5:
    spark.sql("""
SELECT date, sum(quantity * price) AS total_sales
FROM (
    SELECT *, FLOOR(RAND()*20) AS salt FROM sales
) s
JOIN (
    SELECT *, EXPLODE(ARRAY(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)) AS salt FROM items
) i ON s.item_id = i.id and s.salt = i.salt
GROUP BY 1
ORDER BY 2 DESC;""").show(5)

elif case == 6:
    spark.conf.set("spark.sql.adaptive.enabled", True)
    df_f = spark.sql("""
SELECT date, sum(quantity * price) AS total_sales
FROM sales s
JOIN items i ON s.item_id = i.id
GROUP BY 1
ORDER BY 2 DESC;""")
    df_f.show(5)

elif case == 7:
    df_t = spark.sql("""
SELECT date, sum(quantity * price) AS total_sales
FROM sales s
JOIN items i ON s.item_id = i.id
GROUP BY 1
ORDER BY 2 DESC;""")
    df_t.show(5)

input("Waiting ...")
