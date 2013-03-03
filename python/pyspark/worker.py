"""
Worker that receives input from PythonRDD.
"""
import socket
import sys
import traceback
from base64 import standard_b64decode
# CloudPickler needs to be imported so that depicklers are registered using the
# copy_reg module.
from pyspark.accumulators import _accumulatorRegistry
from pyspark.broadcast import Broadcast, _broadcastRegistry
from pyspark.cloudpickle import CloudPickler
from pyspark.files import SparkFiles
from pyspark.serializers import write_with_length, read_with_length, write_int, \
    read_long, read_int, dump_pickle, load_pickle, read_from_pickle_file


def load_obj(f):
    return load_pickle(standard_b64decode(f.readline().strip()))


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("", 0))
    (host, port) = sock.getsockname()
    # Print the host and port so that PythonRDD can figure out which
    # socket to connect to:
    sys.stdout.write(host + "\n")
    sys.stdout.write("%i\n" % port)
    sys.stdout.flush()

    # Accept a single connection:
    sock.listen(1)
    (spark_socket, spark_address) = sock.accept()
    spark_in = spark_socket.makefile('r')
    spark_out = spark_socket.makefile('w')

    split_index = read_int(spark_in)
    spark_files_dir = load_pickle(read_with_length(spark_in))
    SparkFiles._root_directory = spark_files_dir
    SparkFiles._is_running_on_worker = True
    sys.path.append(spark_files_dir)
    num_broadcast_variables = read_int(spark_in)
    for _ in range(num_broadcast_variables):
        bid = read_long(spark_in)
        value = read_with_length(spark_in)
        _broadcastRegistry[bid] = Broadcast(bid, load_pickle(value))
    func = load_obj(spark_in)
    bypassSerializer = load_obj(spark_in)
    if bypassSerializer:
        dumps = lambda x: x
    else:
        dumps = dump_pickle
    iterator = read_from_pickle_file(spark_in)
    try:
        for obj in func(split_index, iterator):
           write_with_length(dumps(obj), spark_out)
    except Exception as e:
        write_int(-2, spark_out)
        write_with_length(traceback.format_exc(), spark_out)
        sys.exit(-1)
    # Mark the beginning of the accumulators section of the output
    write_int(-1, spark_out)
    for aid, accum in _accumulatorRegistry.items():
        write_with_length(dump_pickle((aid, accum._value)), spark_out)


if __name__ == '__main__':
    main()
