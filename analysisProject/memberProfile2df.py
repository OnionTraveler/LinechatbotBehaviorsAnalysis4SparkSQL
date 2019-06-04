#!/usr/bin/python3
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Create a DataFrame from a JSON file
    memberProfile_df = spark.read.csv("hdfs://localhost/user/cloudera/SparkSQLBehaviorsAnalysis4Linechatbot/data/memberProfile4MySQL", header=True)


    


#========================= (Launch Spark by 「Interactive Development Environment」)
# pyspark --master spark://172.21.0.2:7077

#========================= (Launch Spark by 「Shell Script」)
# spark-submit --master spark://172.21.0.2:7077 message_event_analysis.py

