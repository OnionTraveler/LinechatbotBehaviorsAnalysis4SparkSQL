from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import Row

def triggerPeriodGroup(rdd):
    temp = rdd.asDict()
    global i
    if temp["trigger_period"] > 120:
        i+=1
    temp["trigger_period_group"]=i
    return Row(**temp)

if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession \
        .builder \
        .getOrCreate()

    # Create a DataFrame from a JSON file
    event_df = spark.read.json("hdfs://localhost/user/cloudera/SparkSQLBehaviorsAnalysis4Linechatbot/data/eventLog")
     
    
    # Explode all elements in the column (event_df["events"]) -> all record represent every behavior event
    allbehaviors_df = event_df.select(explode(event_df["events"]).alias("events"))
    
    
    # Select columns which will be analyzed later
    clear_df = allbehaviors_df.select(allbehaviors_df["events"]["timestamp"].alias("timestamp"), \
                                      from_unixtime(allbehaviors_df["events"]["timestamp"]/1000, "yyyy-MM-dd HH:mm:ss").alias("event_time"), \
                                      allbehaviors_df["events"]["source"]["userId"].alias("userid"), \
                                      allbehaviors_df["events"]["type"].alias("type"), \
                                      allbehaviors_df["events"]["message"]["text"].alias("messageEvent"), \
                                      allbehaviors_df["events"]["postback"].alias("postbackEvent"))
    
    
    # analysis(staying time)
    # trigger_period in clear_df
    window_clause1 = Window.partitionBy(clear_df["userid"]).orderBy(clear_df["timestamp"])
    
    window_specification1 = window_clause1.rowsBetween(-1, 0)
    
    clear_df = clear_df.withColumn("trigger_period", ((clear_df["timestamp"] - first(clear_df["timestamp"]).over(window_specification1))/1000))
    
    
    # triggerPeriodGroup_df
    i=0; triggerPeriodGroup_df = clear_df.rdd.map(triggerPeriodGroup).toDF(sampleRatio=0.2)
    
    
    # once_staying_time in triggerPeriodGroup_df
    window_clause2 = Window.partitionBy([triggerPeriodGroup_df["userid"], triggerPeriodGroup_df["trigger_period_group"]]) \
                           .orderBy(triggerPeriodGroup_df["trigger_period_group"])
    
    window_specification2 = window_clause2.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    triggerPeriodGroup_df = triggerPeriodGroup_df.withColumn("once_staying_time", \
                            (sum(triggerPeriodGroup_df["trigger_period"]).over(window_specification2) - \
                             max(triggerPeriodGroup_df["trigger_period"]).over(window_specification2))) \
                             .orderBy(triggerPeriodGroup_df["userid"])

    
    # staying_time
    staying_time_df = triggerPeriodGroup_df.groupBy( [triggerPeriodGroup_df["userId"], triggerPeriodGroup_df["trigger_period_group"]]) \
                                           .agg({"once_staying_time":"avg"}) \
                                           .withColumnRenamed("avg(once_staying_time)", "once_staying_time") \
                                           .orderBy([triggerPeriodGroup_df["userId"], triggerPeriodGroup_df["trigger_period_group"]])
    
    staying_time_df = staying_time_df.select(staying_time_df["userId"], \
                                             staying_time_df["once_staying_time"])
    
    
    # save to json file
    staying_time_df.write.json("hdfs://localhost/user/cloudera/SparkSQLBehaviorsAnalysis4Linechatbot/analysisResults/staying_time_analysis")











    # Create a DataFrame from a csv file
    memberProfile_df = spark.read.csv("hdfs://localhost/user/cloudera/SparkSQLBehaviorsAnalysis4Linechatbot/data/memberProfile4MySQL", header=True)

    staying_time_with_memberProfile_df = staying_time_df.join(memberProfile_df, staying_time_df["userId"] == memberProfile_df["line_id"], 'left_outer') \
                                                        .select(staying_time_df["userId"], \
                                                                memberProfile_df["username"], \
                                                                memberProfile_df["sex"], \
                                                                staying_time_df["once_staying_time"]) \
                                                        .fillna({"username":"unknown", "sex":"unknown"})
    
    
    # save to json file
    staying_time_with_memberProfile_df.write.json("hdfs://localhost/user/cloudera/SparkSQLBehaviorsAnalysis4Linechatbot/analysisResults/staying_time_with_memberProfile_df")



