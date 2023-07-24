from pyspark.sql import SparkSession
import threading
import time


def do_job(f1, f2, id, format="json"):
    if format == 'json':
        df1 = spark.read.json(f1)
        df2 = spark.read.json(f2)
    else:
        df1 = spark.read.csv(f1, header=True)
        df2 = spark.read.csv(f2, header=True)

    outputs.append(df1.join(df2, id, "Inner").count())


scheduler_mode = "FIFO"
spark = SparkSession\
        .builder \
        .appName ("FIFO Scheduler Demo") \
        .config("spark.sql.autoBroadcastJoinThreshold", "50B") \
        .getOrCreate()

spark.sparkContext.getConf().set("spark.scheduler.mode", scheduler_mode)
outputs = []

start_time_fifo = time.time()
thread1 = threading.Thread(
    target=do_job,
    args=(
        "small_data/",
        "large_data/",
        "id"
    )
)
thread2 = threading.Thread(
    target=do_job,
    args=(
        "user_event.csv",
        "user_metadata.csv",
        "user_id",
        "csv"
    )
)

thread1.start()
thread2.start()

thread1.join()
thread2.join()

end_time_fifo = time.time()
print(f"Time taken with {scheduler_mode} Scheduler: {(end_time_fifo - start_time_fifo) * 1000:.2f} ms")
print (outputs)

input("Waiting ...")
