import pyspark
from pyspark.sql import SparkSession


#read data from socket
def start_streaming(spark):
    #python syntax for multi-line by enclosing in brackets
    stream_df = (spark.readStream.format('socket')
                                .option('host', 'localhost')
                                .option('port', 9999).load())
    
    query = stream_df.writeStream.format('console').start()
    query.awaitTermination()

#create session
if __name__ == '__main__':
    spark = SparkSession.builder.appName('SocketStreamConsumer').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    start_streaming(spark)
