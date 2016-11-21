
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.streaming import StreamingContext
import sys, time
from operator import add

streaming_context = StreamingContext(sc, 10)

lines = KinesisUtils.createStream(streaming_context, 'SparkTestLeaseTable', 'SparkTest', 'kinesis.us-east-1.amazonaws.com', 'us-east-1', InitialPositionInStream.LATEST, 100000, StorageLevel.MEMORY_AND_DISK_2,)

#top5 = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1)).reduceByKey(add).sortByKey().top(5)
top5 = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x,1)).reduceByKey(add).transform(lambda x: x)
top5.saveAsTextFiles(‘/Users/thomad/projects/spark1.6’)

streaming_context.start()
streaming_context.awaitTermination()
