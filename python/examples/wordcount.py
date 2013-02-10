import sys
from operator import add

from pyspark import SparkContext
from pyspark.serializers import MarshalSerializer


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print >> sys.stderr, \
            "Usage: PythonWordCount <master> <file>"
        exit(-1)
    sc = SparkContext(sys.argv[1], "PythonWordCount", serializer=MarshalSerializer)
    lines = sc.textFile(sys.argv[2], 1)
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print "%s : %i" % (word, count)
