from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, MapType, ArrayType
from pyspark.sql import functions as F

spark = SparkSession.builder\
        .master('local[*]')\
        .appName('Test App')\
        .getOrCreate()

data2 = spark.sparkContext.parallelize([
    (1, "New_York"),
    (2, "Miami"),
    (2, "Miami"),
    (2, "Miami"),
    (3, "Miami"),
    (3, "Chicago"),
    (4, "Chicago")
])

rdd = spark.sparkContext.parallelize([[[1]],[[2]],[[3]],[[4]],[[5]]])

# map_rdd = rdd.map(lambda x: (x**2))

# print(map_rdd.collect())

# flatmap_rdd = rdd.flatMap(lambda x:x[0])

# print(flatmap_rdd.collect())

# rdd_mappartitions = rdd.mapPartitions(lambda x:x)
# print(rdd_mappartitions.glom().collect())

rdd_group = data2.groupByKey().collect()

print(rdd_group.forEach(F.println))