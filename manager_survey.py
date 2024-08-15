from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T #StructType, T.StructField, StringType, LongType, IntegerType, MapType, ArrayType
from pyspark.sql import functions as F
from currency_converter import CurrencyConverter
import boto3
import pandas

# spark = SparkSession.builder\
#         .master('local[*]')\
#         .appName('Test App')\
#         .getOrCreate()

spark = SparkSession.builder.appName("Manager Survey").getOrCreate()


schema = T.StructType([
    T.StructField("Timestamp", T.StringType()), 
    T.StructField("age_range", T.StringType()), 
    T.StructField("industry", T.StringType()), 
    T.StructField("job_title", T.StringType()), 
    T.StructField("job_title_context", T.StringType()), 
    T.StructField("annual_salary", T.StringType()), 
    T.StructField("additional_monetory_benefits", T.StringType()), 
    T.StructField("currency", T.StringType()), 
    T.StructField("other_currency", T.StringType()), 
    T.StructField("salary_additional_context", T.StringType()), 
    T.StructField("work_country", T.StringType()), 
    T.StructField("US_state", T.StringType()), 
    T.StructField("US_city", T.StringType()), 
    T.StructField("overall_total_experience", T.StringType()), 
    T.StructField("relevant_professional_experience", T.StringType()), 
    T.StructField("highest_education", T.StringType()), 
    T.StructField("gender", T.StringType()), 
    T.StructField("race", T.StringType())
    ])


df = spark.read.csv("./Data/Ask_A_Manager_Salary_Survey_2021_responses_1.csv", schema=schema, header=True)

# column_list = df.columns

# schema_list=[]

# for col in column_list:
#     schema_list.append(f'T.StructField("{col}", T.StringType())')

# print(schema_list)


## Transformation 1 : Which industry pays the most?
# S1 : Remove other currency records, Remove blank Industry values, Remove under 18 age records
# S2 : Convert all annual_salary values to USD and store in new column salary_in_USD
# S3 : Take average of salary_in_USD by grouping with Industry column
# Convert NULL to default value for annual salary

## S1 - Filter and Clean
filtered_df = df.withColumn('annual_salary', F.regexp_replace(df['annual_salary'],',','').cast(T.FloatType()))
filtered_df = filtered_df.filter((F.trim(df['industry']) != '')\
                                    & (F.trim(F.lower(df['currency']))!='other')\
                                    & (F.trim(df['age_range'])=='under 18')) # & (F.length(F.col("currency")) == 3))
                                    
cleaned_df = filtered_df.na.fill(value='0', subset=['additional_monetory_benefits'])
cleaned_df = cleaned_df.fillna(value='', subset=['other_currency'])


## S2
# UDF to convert currencies
c = CurrencyConverter()
def convert_currency(amt, currency):
    if currency != 'USD':
        return c.convert(amt, currency, 'USD')
    else:
        return amt

currency_converterUDF = F.udf(convert_currency, T.FloatType())
# currency_converterUDF = F.udf(lambda x, y: c.convert(x, y, 'USD'), T.FloatType())

us_currency_value = cleaned_df.withColumn('salary_in_USD', F.round(currency_converterUDF('annual_salary', 'currency'), 2))
# us_currency_value.select('annual_salary', 'additional_monetory_benefits', 'currency', 'other_currency', 'salary_in_USD').show()

# S3: GroupBy dataset on the industry
industry_group = us_currency_value.groupBy('industry').agg(F.avg('salary_in_USD').alias("average_salary_USD"))

industry_group = industry_group.withColumn('average_salary_USD', F.round('average_salary_USD', 2)).orderBy('average_salary_USD', ascending=False)

# df.show(5, truncate=False)
# industry_group.show(5, truncate=False)

# industry_group.write.csv('D:/Data Engineering/crdh/Data/output/averge_salary_by_industry', mode='overwrite', header=True)

pd_df = industry_group.toPandas()
pd_df.to_csv('D:/Data Engineering/crdh/Data/output/averge_salary_by_industry.csv', header=True, index=False)

s3_client = boto3.client('s3')
# s3_client.put_object(Bucket='manish-awsbucket-v1', Body=bytes(industry_group, encoding='utf-8', Key="sales_data_processed/manager_survey.csv"))
s3_client.upload_file('D:/Data Engineering/crdh/Data/output/averge_salary_by_industry.csv', 'manish-awsbucket-v1', 'sales_data_processed/manager_survey.csv')