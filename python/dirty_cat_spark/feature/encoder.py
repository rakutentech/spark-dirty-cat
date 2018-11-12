from pyspark import since, keyword_only, SparkContext
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, HasHandleInvalid
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import _jvm
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaParams, JavaWrapper

from dirty_cat_spark.utils.java_reader import CustomJavaMLReader



class SimilarityEncoder(JavaEstimator, HasInputCol, HasOutputCol,
                        HasHandleInvalid, JavaMLReadable, JavaMLWritable):
    """

    >>> encoder = SimilarityEncoder(inputCol="names", outputCol="encoderNames")
    """

    vocabSize = Param(Params._dummy(), "vocabSize", "",
                      typeConverter=TypeConverters.toInt)

    nGramSize = Param(Params._dummy(), "nGramSize", "",
                      typeConverter=TypeConverters.toInt)

    similarityType = Param(Params._dummy(), "similarityType", "",
                           typeConverter=TypeConverters.toString)

    handleInvalid = Param(Params._dummy(), "handleInvalid", "",
                          typeConverter=TypeConverters.toString)

    stringOrderType = Param(Params._dummy(), "stringOrderType", "",
                            typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None,
                 nGramSize=3, similarityType="nGram",
                 handleInvalid="keep",
                 stringOrderType="frequencyDesc",
                 vocabSize=100):
        """
        __init__(self, inputCol=None, outputCol=None,
                 nGramSize=3, similarityType="nGram",
                 handleInvalid="keep", stringOrderType="frequencyDesc",
                 vocabSize=100)
        """
        super(SimilarityEncoder, self).__init__()

        self._java_obj = self._new_java_obj(
            "com.rakuten.dirty_cat.feature.SimilarityEncoder", self.uid)

        self._setDefault(nGramSize=3,
                         # vocabSize=100,
                         stringOrderType="frequencyDesc",
                         handleInvalid="keep",
                         similarityType="nGram")

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None,
                  nGramSize=3, similarityType="nGram",
                  handleInvalid="keep",
                  stringOrderType="frequencyDesc",
                  vocabSize=100):
        """
        setParams(self, inputCol=None, outputCol=None, nGramSize=3,
                 similarityType="nGram", handleInvalid="keep",
                 stringOrderType="frequencyDesc", vocabSize=100)

        Set the params for the SimilarityEncoder
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)


    def setStringOrderType(self, value):
        return self._set(stringOrderType=value)

    def setSimilarityType(self, value):
        return self._set(similarityType=value)

    def setNGramSize(self, value):
        return self._set(nGramSize=value)

    def setVocabSize(self, value):
        return self._set(vocabSize=value)


    def getStringOrderType(self):
        return self.getOrDefault(self.stringOrderType)

    def getSimilarityType(self):
        return self.getOrDefault(self.similarityType)

    def getNGramSize(self):
        return self.getOrDefault(self.nGramSize)

    def getVocabSize(self):
        return self.getOrDefault(self.vocabSize)


    def _create_model(self, java_model):
        return SimilarityEncoderModel(java_model)



class SimilarityEncoderModel(JavaModel, JavaMLReadable, JavaMLWritable):
    """Model fitted by :py:class:`SimilarityEncoder`. """

    @property
    def vocabularyReference(self):
        """
        """
        return self._call_java("vocabularyReference")

#     # @classmethod
#     # def from_vocabularyReference(cls, vocabularyReference, inputCol,
#     #                              outputCol=None, nGramSize=None,
#     #                              similarityType=None, handleInvalid=None,
#     #                              stringOrderType=None, vocabSize=None):
#     #     """
#     #     Construct the model directly from an array of label strings,
#     #     requires an active SparkContext.
#     #     """
#     #     sc = SparkContext._active_spark_context
#     #     java_class = sc._gateway.jvm.java.lang.String
#     #     jVocabularyReference = SimilarityEncoderModel._new_java_array(
#     #         vocabularyReference, java_class)
#     #     model = SimilarityEncoderModel._create_from_java_class(
#     #         'dirty_cat.feature.SimilarityEncoderModel', jVocabularyReference)
#     #     model.setInputCol(inputCol)
#     #     if outputCol is not None:
#     #         model.setOutputCol(outputCol)
#     #     if nGramSize is not None:
#     #         model.setNGramSize(nGramSize)
#     #     if similarityType is not None:
#     #         model.setSimilarityType(similarityType)
#     #     if handleInvalid is not None:
#     #         model.setHandleInvalid(handleInvalid)
#     #     if stringOrderType is not None:
#     #         model.setStringOrderType(stringOrderType)
#     #     if vocabSize is not None:
#     #         model.setVocabSize(vocabSize)
#     #     return model


# #     @staticmethod
# #     def _from_java(java_stage):
# #         """
# #         Given a Java object, create and return a Python wrapper of it.
# #         Used for ML persistence.
# #         Meta-algorithms such as Pipeline should override this method as a classmethod.
# #         """
# #         # Generate a default new instance from the stage_name class.
# #         py_type =SimilarityEncoderModel
# #         if issubclass(py_type, JavaParams):
# #             # Load information from java_stage to the instance.
# #             py_stage = py_type()
# #             py_stage._java_obj = java_stage
# #             py_stage._resetUid(java_stage.uid())
# #             py_stage._transfer_params_from_java()

# #         return py_stage

#     # @classmethod
#     # def read(cls):
#     #     """Returns an MLReader instance for this class."""
#     #     return CustomJavaMLReader(
#     #         cls, 'dirty_cat.feature.SimilarityEncoderModel')
