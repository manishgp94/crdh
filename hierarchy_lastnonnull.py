from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, MapType, ArrayType
from pyspark.sql import functions as F

spark = SparkSession.builder\
        .master('local[*]')\
        .appName('Test App')\
        .getOrCreate()

df = spark.createDataFrame(
    [
        ("D1", "D2", "H1", None, None),
        ("D1", "D2", "H1", "H2", None),
        ("D1", "D2", "H1", "H2", "H3")
    ],
    ["Dimension1", "Dimension2", "Hierarchy1", "Hierarchy2", "Hierarchy3"]
)


def get_hierarchy(*cols):
    for col in reversed(cols):
        if col is not None:
            return col
    return None

get_hierarchyUDF = F.udf(get_hierarchy, StringType())

hierarchy_columns = [F.col(x) for x in df.columns if x.startswith("Hierarchy")]
new_df = df.withColumn("Hierarchy", get_hierarchyUDF(*hierarchy_columns))

new_df.select('Dimension1','Dimension2','Hierarchy').show()
# df.show()