from time import sleep

import pyspark
from pyspark.sql.streaming import StreamingQueryException
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from config.config import config

#streaming client
#read data from socket
def start_streaming(spark):
    topic = 'yelp_reviews_topic'
    limiter = 10
    running_count = 0
    print(topic)
    while running_count < limiter:
        try:
            stream_df = (spark.readStream.format("socket")
                                    .option("host", "0.0.0.0")
                                    .option("port", 9999)
                                    .load()
                                    )

            schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType())
            ])
            #logs are currently all in the value column
            stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))
            print(stream_df)

            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")
            print(kafka_df)
            query = (kafka_df.writeStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                   .option("kafka.security.protocol", config['kafka']['security.protocol'])
                   .option('kafka.sasl.mechanism', config['kafka']['sasl.mechanisms'])
                   .option('kafka.sasl.jaas.config',
                           'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                           'password="{password}";'.format(
                               username=config['kafka']['sasl.username'],
                               password=config['kafka']['sasl.password']
                           ))
                   .option('checkpointLocation', '/tmp/checkpoint')
                   .option('topic', topic)
                   .start()
                )
            try:
                query.awaitTermination()
            except StreamingQueryException as e:
                query.stop()
                print("connection stopped due to streaming query issues")
                print(e)
            finally:
                running_count += 1

        except Exception as e:
            print(f'Exception encountered: {e}. Retrying in 10 seconds')
            sleep(10)
        finally:
            running_count += 1


#create session
if __name__ == '__main__':
    spark = SparkSession.builder.appName('SocketStreamConsumer').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    start_streaming(spark)
