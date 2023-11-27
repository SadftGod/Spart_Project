from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t

from app.default import info_view , read, write


def main():
   spark_session = (SparkSession.builder
                                 .master("local")
                                 .appName("Yelp Analisys")
                                 .config(conf=SparkConf())
                                 .getOrCreate())
   path = "/yelp_data/"
    
   business_df = read(spark_session, f"{path}/yelp_academic_dataset_business.json")
   checkin_df = read(spark_session, f"{path}/yelp_academic_dataset_checkin.json")
   review_df = read(spark_session, f"{path}/yelp_academic_dataset_review.json")
   tip_df = read(spark_session, f"{path}/yelp_academic_dataset_tip.json")
   user_df = read(spark_session, f"{path}/yelp_academic_dataset_user.json")

   info_view(business_df)
   info_view(checkin_df)
   info_view(review_df)
   info_view(tip_df)
   info_view(user_df)
   
main()