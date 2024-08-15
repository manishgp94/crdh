from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, MapType, ArrayType
from pyspark.sql import functions as F

spark = SparkSession.builder\
        .master('local[*]')\
        .appName('Test App')\
        .getOrCreate()


inner_struct = StructType([
        StructField("storeId", StringType()),
        StructField("storeIdHashed", StringType()),
        StructField("lastUpdated", StringType()),
        StructField("sum1d", StringType())
])

my_feature_name = StructType([
        StructField("name", StringType()),
        StructField("first_profiles", MapType(StringType(), inner_struct))
])

# my_feature_name = StructType([
#         StructField("name", StringType()),
#         StructField("first_profiles", MapType(StringType(), StructField(["storeId", "storeIdHashed", "lastUpdated","sum1d"], ArrayType(StringType()))))
# ])

# schema = StructType([
#     StructField('Name', StringType()),
#     StructField('Properties', MapType(StringType(), ArrayType(StringType())))
# ])

# data1 = [({"FP1": ["S1", "SH1", "20230401", 1230]}),({"FP2": ["S1", "SH1", "20230401", 1230]})]

# data2 = [({"FP1": "S1"}),({"FP2": "S1"})]

# data3 = [('Manish',{'company': [["S1", "SH1", "20230401", 1230]], 'Salary':[["S1", "SH1", "20230401", 1230]]}),
#         ('Surbhi',{'company': [["S1", "SH1", "20230401", 1230]], 'Salary':[["S1", "SH1", "20230401", 1230]]})]

data4 = [('Manish',{'company': ["S1", "SH1", "20230401", "1230"], 'Salary':["S1", "SH1", "20230401", "1230"]}),
        ('Surbhi',{'company': ["S1", "SH1", "20230401", "1230"], 'Salary':["S1", "SH1", "20230401", "1230"]})]


# df = spark.read.json("./utils/test_data.json", multiLine=True)

df = spark.createDataFrame(data4, my_feature_name)

df.printSchema()
# df = df.select('name',F.explode(F.map_values(df['first_profiles'])))

df = df.select('name',(F.map_values(df.first_profiles)).alias("details"))

df = df.select('name',df.details[0])
df.show(truncate=False)

