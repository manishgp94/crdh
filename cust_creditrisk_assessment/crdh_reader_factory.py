from pyspark.sql import SparkSession

class DataSource:
    def __init__(self, path):
        self.path = path
        self.spark = SparkSession.builder\
                        .appName("CRDH")\
                        .config("spark.jars", "C:\\mysql-connector-j-9.0.0\\mysql-connector-j-9.0.0.jar")\
                        .getOrCreate()
        self.properties = {
            "user":"root",
            "password":"1234",
            "driver":"com.mysql.cj.jdbc.Driver"
        }

    def get_dataframe(self):
        pass

class CSVDataSource(DataSource):
    def get_dataframe(self):
        csv_df = self.spark.read\
                    .csv(self.path, header=True, inferSchema=True)

        return csv_df
    
class DBDataSource(DataSource):
    def get_dataframe(self):
        db_df = self.spark.read\
                    .jdbc("jdbc:mysql://localhost:3306/crdh_db", self.path, properties=self.properties)

        return db_df
    

def get_data_source(df_type, path):
    if(df_type=="csv"):
        return CSVDataSource(path).get_dataframe()
    elif(df_type=="table"):
        return DBDataSource(path).get_dataframe()
    else:
        raise ValueError("No Data Type Present")

    

