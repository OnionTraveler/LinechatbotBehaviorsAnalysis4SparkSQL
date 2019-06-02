from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Create a DataFrame from a JSON file
    memberProfile_df = spark.read.csv("hdfs://localhost/user/cloudera/SparkSQLBehaviorsAnalysis4Linechatbot/data/memberProfile4MySQL", header=True)


