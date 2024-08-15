
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, MapType, ArrayType
from pyspark.sql import functions as F

spark = SparkSession.builder\
        .master('local[*]')\
        .appName('Test App')\
        .getOrCreate()


sales = [
    ("apple", 10),
    ("banana", 20),
    ("apple", 30),
    ("apple", 50),
    ("banana", 40),
    ("orange", 50)
]

rdd = spark.sparkContext.parallelize(sales)


rdd_group = rdd.groupByKey()

rdd_group = rdd_group.mapValues(sum).collect()

print(rdd_group)

rdd_reduce = rdd.reduceByKey(lambda x,y: (x+y)/2)
print(rdd_reduce.collect())