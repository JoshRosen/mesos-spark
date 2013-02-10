"""
Worker that receives input from Piped RDD.
"""
import os
import sys
import traceback
from base64 import standard_b64decode
# CloudPickler needs to be imported so that depicklers are registered using the
# copy_reg module.
from pyspark.accumulators import _accumulatorRegistry
from pyspark.broadcast import Broadcast, _broadcastRegistry
from pyspark.cloudpickle import CloudPickler
from pyspark.files import SparkFiles
from pyspark.serializers import write_int, read_long, read_int, \
    PickleSerializer


# Redirect stdout to stderr so that users must return values from functions.
old_stdout = os.fdopen(os.dup(1), 'w')
os.dup2(2, 1)


def load_obj():
    decoded = standard_b64decode(sys.stdin.readline().strip())
    return PickleSerializer.loads(decoded)


def main():
    split_index = read_int(sys.stdin)
    spark_files_dir = PickleSerializer.read_with_length(sys.stdin)
    SparkFiles._root_directory = spark_files_dir
    SparkFiles._is_running_on_worker = True
    sys.path.append(spark_files_dir)
    num_broadcast_variables = read_int(sys.stdin)
    for _ in range(num_broadcast_variables):
        bid = read_long(sys.stdin)
        value = PickleSerializer.read_with_length(sys.stdin)
        _broadcastRegistry[bid] = Broadcast(bid, value)
    func = load_obj()
    serializer = load_obj()
    iterator = serializer.read_from_file(sys.stdin)
    try:
        serializer.write_to_file(func(split_index, iterator), old_stdout)
    except Exception as e:
        write_int(-2, old_stdout)
        formatted_traceback = traceback.format_exc()
        write_int(len(formatted_traceback), old_stdout)
        old_stdout.write(formatted_traceback)
        sys.exit(-1)
    # Mark the beginning of the accumulators section of the output
    write_int(-1, old_stdout)
    for aid, accum in _accumulatorRegistry.items():
        PickleSerializer.write_with_length((aid, accum._value), old_stdout)


if __name__ == '__main__':
    main()
