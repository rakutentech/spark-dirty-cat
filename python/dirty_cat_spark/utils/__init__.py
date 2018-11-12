import py4j.protocol
from py4j.protocol import Py4JJavaError
from py4j.java_gateway import JavaObject
from py4j.java_collections import JavaArray, JavaList, JavaMap

from pyspark import RDD, SparkContext
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql import DataFrame, SQLContext

# Hack for support float('inf') in Py4j
_old_smart_decode = py4j.protocol.smart_decode

_float_str_mapping = {
    'nan': 'NaN',
    'inf': 'Infinity',
    '-inf': '-Infinity',
}


def _new_smart_decode(obj):
    if isinstance(obj, float):
        s = str(obj)
        return _float_str_mapping.get(s, s)
    return _old_smart_decode(obj)

py4j.protocol.smart_decode = _new_smart_decode


_picklable_classes = [
    'SparseVector',
    'DenseVector',
    'SparseMatrix',
    'DenseMatrix',
]


def _java2py(sc, r, encoding="bytes"):
    if isinstance(r, JavaObject):
        clsName = r.getClass().getSimpleName()
        # convert RDD into JavaRDD
        if clsName != 'JavaRDD' and clsName.endswith("RDD"):
            r = r.toJavaRDD()
            clsName = 'JavaRDD'

        if clsName == 'JavaRDD':
            jrdd = sc._jvm.org.apache.spark.ml.python.MLSerDe.javaToPython(r)
            return RDD(jrdd, sc)

        if clsName == 'Dataset':
            return DataFrame(r, SQLContext.getOrCreate(sc))

        if clsName in _picklable_classes:
            r = sc._jvm.org.apache.spark.ml.python.MLSerDe.dumps(r)

        elif isinstance(r, (JavaArray, JavaList, JavaMap)):
            try:
                r = sc._jvm.org.apache.spark.ml.python.MLSerDe.dumps(r)
            except Py4JJavaError:
                pass  # not pickable

    if isinstance(r, (bytearray, bytes)):
        r = PickleSerializer().loads(bytes(r), encoding=encoding)
    return r

