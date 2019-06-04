from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
    


    # Show how many users
    clear_df.agg(countDistinct(clear_df["userid"])).show()  # 48

    # messageEvent analysis
    clear_df.drop(clear_df["postbackEvent"]).filter(clear_df["messageEvent"]=="查看個人資料").count()  # 243
    clear_df.drop(clear_df["postbackEvent"]).filter(clear_df["messageEvent"]=="情境輸入").count()  # 48
    clear_df.drop(clear_df["postbackEvent"]).filter(clear_df["messageEvent"]=="開啟衣櫃").count()  # 66
    clear_df.drop(clear_df["postbackEvent"]).filter(clear_df["messageEvent"]=="我的明星臉").count()  # 88
    
    clear_df.drop(clear_df["postbackEvent"]).filter(clear_df["messageEvent"]=="加入會員").count()  # 96
    clear_df.drop(clear_df["postbackEvent"]).filter(clear_df["messageEvent"]=="關於我").count()  # 50
    
    
    clear_df.drop(clear_df["postbackEvent"]).filter(clear_df["messageEvent"]=="功能說明").count()  # 11
    
    freeInputMessageEvent_df = clear_df.drop(clear_df["postbackEvent"]).filter((clear_df["messageEvent"]!="查看個人資料") & \
                                                                               (clear_df["messageEvent"]!="情境輸入") & \
                                                                               (clear_df["messageEvent"]!="開啟衣櫃") & \
                                                                               (clear_df["messageEvent"]!="我的明星臉")) \
                                                                       .filter((clear_df["messageEvent"]!="加入會員") & \
                                                                               (clear_df["messageEvent"]!="關於我") & \
                                                                               (clear_df["messageEvent"]!="功能說明")) \
                                                                       .filter((clear_df["messageEvent"]!="不是T-shirt，請換選項") & \
                                                                               (clear_df["messageEvent"]!="不是襯衫，請換選項") & \
                                                                               (clear_df["messageEvent"]!="不是西裝外套，請換選項") & \
                                                                               (clear_df["messageEvent"]!="不是洋裝，請換選項") & \
                                                                               (clear_df["messageEvent"]!="不是背心，請換選項")) \
                                                                       .filter((clear_df["messageEvent"]!="進行運動相關的活動，開始推薦") & \
                                                                               (clear_df["messageEvent"]!="進行休閒相關的活動，開始推薦") & \
                                                                               (clear_df["messageEvent"]!="進行正式相關的活動，開始推薦")) \
                                                                       .filter((clear_df["messageEvent"]!="不是運動相關的活動，請換選項") & \
                                                                               (clear_df["messageEvent"]!="不是休閒相關的活動，請換選項") & \
                                                                               (clear_df["messageEvent"]!="不是正式相關的活動，請換選項")) \
                                                                       .filter((clear_df["messageEvent"]!="下一頁") & \
                                                                               (clear_df["messageEvent"]!="null"))  
    
    freeInputMessageEvent_df.count()  # 100
    

    # save to json file
    freeInputMessageEvent_df.write.json("hdfs://localhost/user/cloudera/SparkSQLBehaviorsAnalysis4Linechatbot/analysisResults/message_event_analysis")





#========================= (Launch a spark by 「interactive environment」)
# pyspark --master spark://172.21.0.2:7077

