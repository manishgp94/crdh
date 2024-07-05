from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, to_date, lower


class TableLoader:

    def __init__(self):
        self.spark = SparkSession.builder\
            .appName("CSV_TO_TABLE")\
            .config("spark.jars", "K:\\Manish\\Data Engineering\\Data\\Financial\\mysql-connector-j-9.0.0\\mysql-connector-j-9.0.0\\mysql-connector-j-9.0.0.jar")\
            .getOrCreate()


    def csv_to_table_loader(self, csv_file_with_path, table_name):
        url = "jdbc:mysql://localhost:3306/crdh_db"
        properties = {
            "user":"root",
            "password":"1234",
            "driver":"com.mysql.cj.jdbc.Driver"
        }

        csv_df = self.spark.read.csv(csv_file_with_path, header=True, inferSchema=True)

        for colname in csv_df.schema.names:
            if "date" in colname.lower():
                csv_df = csv_df.withColumn(colname, to_date(csv_df[colname],'dd-MM-yyyy'))
        # csv_df = csv_df.show()

        csv_df.write.jdbc(url, table_name, mode="append", properties=properties)


tableLoader = TableLoader()
# tableLoader.csv_to_table_loader("K:\\Manish\\Data Engineering\\Data\\Financial\\customer_details.csv", "customers")
tableLoader.csv_to_table_loader("K:\\Manish\\Data Engineering\\Data\\Financial\\accounts_details.csv", "accounts")