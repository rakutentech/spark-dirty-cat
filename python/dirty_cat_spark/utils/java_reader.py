import sys

from pyspark.ml.util import MLReader, _jvm


class CustomJavaMLReader(MLReader):
    """
    (Custom) Specialization of :py:class:`MLReader` for :py:class:`JavaParams` types
    """

    def __init__(self, clazz, java_class):
        self._clazz = clazz
        self._jread = self._load_java_obj(java_class).read()

    def load(self, path):
        """Load the ML instance from the input path."""
        java_obj = self._jread.load(path)
        return self._clazz._from_java(java_obj)

    @classmethod
    def _load_java_obj(cls, java_class):
        """Load the peer Java object of the ML instance."""
        java_obj = _jvm()
        for name in java_class.split("."):
            java_obj = getattr(java_obj, name)
        return java_obj
