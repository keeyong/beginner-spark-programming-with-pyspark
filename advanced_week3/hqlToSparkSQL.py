import sqlglot

hive_sqls = [
    """CREATE TEMPORARY TABLE aa AS SELECT * FROM test""",
    """LOAD DATA LOCAL INPATH '/home/dikshant/Documents/data.csv' INTO TABLE student;""",
    """CREATE TABLE IF NOT EXISTS student(
Student_Name STRING,
Student_Rollno INT,
Student_Marks FLOAT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';"""
]

for idx, hive_sql in enumerate(hive_sqls):
    spark_sql = sqlglot.transpile(
        hive_sql,
        read="hive",
        write="spark",
        pretty=True
    )[0]
    print(idx, hive_sql, spark_sql)
