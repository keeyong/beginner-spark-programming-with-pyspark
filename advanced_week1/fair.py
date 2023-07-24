from pyspark.sql import SparkSession
import threading
import time


def do_job(f1, f2, id, pool_name, format="json"):
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool_name)
    print(spark.sparkContext.getLocalProperty("spark.scheduler.pool"))
    if format == 'json':
        df1 = spark.read.json(f1)
        df2 = spark.read.json(f2)
    else:
        df1 = spark.read.csv(f1, header=True)
        df2 = spark.read.csv(f2, header=True)

    outputs.append(df1.join(df2, id, "Inner").count())


spark = SparkSession\
    .builder \
    .appName ("Fair Scheduler Demo") \
    .config("spark.sql.autoBroadcastJoinThreshold", "50B") \
    .config("spark.scheduler.mode", "FAIR") \
    .config("spark.scheduler.allocation.file", "fair.xml") \
    .getOrCreate()

outputs = []

start_time_fifo = time.time()
thread1 = threading.Thread(
    target=do_job,
    args=(
        "small_data/",
        "large_data/",
        "id",
        "production"
    )
)
thread2 = threading.Thread(
    target=do_job,
    args=(
        "user_event.csv",
        "user_metadata.csv",
        "user_id",
        "test",
        "csv"
    )
)

thread1.start()
thread2.start()

thread1.join()
thread2.join()

end_time_fifo = time.time()
print(f"Time taken with FAIR Scheduler: {(end_time_fifo - start_time_fifo) * 1000:.2f} ms")
print (outputs)

input("Waiting ...")
