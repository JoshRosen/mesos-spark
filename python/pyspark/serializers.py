from pyspark import cloudpickle
import struct
import cPickle
import marshal


__all__ = ["Serializer", "NoOpSerializer", "PickleSerializer",
           "MarshalSerializer"]


class Serializer(object):
    """
    Base class for serializers.  Custom serializers should implement C{loads}
    and C{dumps}.
    """

    @classmethod
    def write_with_length(cls, obj, stream):
        serialized = cls.dumps(obj)
        write_int(len(serialized), stream)
        stream.write(serialized)

    @classmethod
    def read_with_length(cls, stream):
        length = read_int(stream)
        obj = stream.read(length)
        if obj == "":
            raise EOFError
        return cls.loads(obj)

    @classmethod
    def read_from_file(cls, stream):
        try:
            while True:
                yield cls.read_with_length(stream)
        except EOFError:
            return

    @classmethod
    def write_to_file(cls, iterator, stream):
        for obj in iterator:
            cls.write_with_length(obj, stream)

    @staticmethod
    def dumps(obj):
        raise NotImplementedError

    @staticmethod
    def loads(obj):
        raise NotImplementedError


class InputOutputSerializer(Serializer):

    def __init__(self, input_serializer, output_serializer):
        self.input_serializer = input_serializer
        self.output_serializer = output_serializer
        self.loads = input_serializer.loads
        self.dumps = output_serializer.dumps
        self.write_to_file = output_serializer.write_to_file
        self.read_from_file = input_serializer.read_from_file
        self.write_with_length = output_serializer.write_with_length
        self.read_with_length = input_serializer.read_with_length

    def __eq__(self, other):
        return isinstance(other, InputOutputSerializer) and \
            other.input_serializer == self.input_serializer and \
            other.output_serializer == self.output_serializer

    def __ne__(self, other):
        return not self == other



class BatchedSerializer(Serializer):

    def __init__(self, serializer, batchSize):
        self.serializer = serializer
        self.batchSize = batchSize

    def dumps(self, obj):
        return self.serializer.dumps(obj)

    def loads(self, obj):
        return self.serializer.loads(obj)

    def write_to_file(self, iterator, stream):
        if isinstance(iterator, basestring):
            iterator = [iterator]
        if self.batchSize == -1:
            self.serializer.write_with_length(list(iterator), stream)
        else:
            items = []
            count = 0
            for item in iterator:
                items.append(item)
                count += 1
                if count == self.batchSize:
                    self.serializer.write_with_length(items, stream)
                    items = []
                    count = 0
            if items:
                self.serializer.write_with_length(items, stream)

    def read_from_file(self, stream):
        for batch in self.serializer.read_from_file(stream):
            for item in batch:
                yield item

    def __eq__(self, other):
        return isinstance(other, BatchedSerializer) and \
            other.serializer == self.serializer

    def __ne__(self, other):
        return not self == other


class NoOpSerializer(Serializer):

    @staticmethod
    def dumps(obj):
        return obj

    @staticmethod
    def loads(obj):
        return obj


class PickleSerializer(Serializer):

    @staticmethod
    def dumps(obj):
        return cPickle.dumps(obj, 2)

    loads = cPickle.loads


class MarshalSerializer(Serializer):

    dumps = marshal.dumps

    loads = marshal.loads


def read_long(stream):
    length = stream.read(8)
    if length == "":
        raise EOFError
    return struct.unpack("!q", length)[0]


def read_int(stream):
    length = stream.read(4)
    if length == "":
        raise EOFError
    return struct.unpack("!i", length)[0]


def write_int(value, stream):
    stream.write(struct.pack("!i", value))
