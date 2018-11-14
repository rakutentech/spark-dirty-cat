/*
 * Licensed to the Rakuten Institute of Technology and Andres Hoyos-Idrobo 
 * under one or more contributor license agreements.  
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 * Rakuten Institute of technology and Andres Hoyos-Idrobo licenses this file 
 * to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rakuten.dirty_cat.feature

import com.rakuten.dirty_cat.persistence.{DefaultParamsReader, DefaultParamsWriter}
import com.rakuten.dirty_cat.util.StringSimilarity
import com.rakuten.dirty_cat.util.collection.OpenHashMap

import org.json4s.JsonAST.{JObject}
import org.apache.spark.ml.linalg.{Vector, Vectors}
// Replace this by VectorUDT
import org.apache.spark.ml.linalg.SQLDataTypes.{VectorType}

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.{Dataset, DataFrame}
//
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructType, StructField}

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.util.{MLWritable, MLReadable, DefaultParamsWritable, DefaultParamsReadable}
import org.apache.spark.ml.util.{MLWriter, MLReader}
import org.apache.spark.ml.param.shared._
import org.apache.spark.SparkException

import org.apache.spark.ml.param.{ParamValidators, ParamPair, Param, Params, ParamMap, DoubleParam, IntParam}

import scala.collection.JavaConverters._
import java.lang.{Double => JDouble, Integer => JInt, String => JString}
import java.util.{NoSuchElementException, Map => JMap}
import scala.language.implicitConversions




private[feature] trait SimilarityBase extends Params with HasInputCol with HasOutputCol with HasHandleInvalid{

  final val nGramSize = new IntParam(this, "nGramSize", "")

  final val vocabSize = new IntParam(this, "vocabSize", "Number of dimensions of the encoding")

  override val handleInvalid = new Param[String](this, "handleInvalid",
    "How to handle invalid data (unseen labels or NULL values). " +
    "Options are 'skip' (filter out rows with invalid data), error (throw an error), " +
    "or 'keep' (put invalid data in a special additional bucket, at index numLabels).",
ParamValidators.inArray(SimilarityEncoder.supportedHandleInvalids))

  final val similarityType = new Param[String](this, "similarityType", "" + 
     s"Supported options: ${SimilarityEncoder.supportedStringOrderType.mkString(", ")}.",
ParamValidators.inArray(SimilarityEncoder.supportedSimilarityType))  
   

  final val stringOrderType = new Param[String](this, "stringOrderType",
    "How to order labels of string column. " +
    "The first label after ordering is assigned an index of 0. " +
    s"Supported options: ${SimilarityEncoder.supportedStringOrderType.mkString(", ")}.",
ParamValidators.inArray(SimilarityEncoder.supportedStringOrderType))  
  
  final def getNGramSize: Int = $(nGramSize)
  
  final def getVocabSize: Int = $(vocabSize)

  final def getSimilarityType: String = $(similarityType)

  final def getStringOrderType: String = $(stringOrderType)


  setDefault(nGramSize -> 3,
    similarityType -> SimilarityEncoder.nGram, 
    vocabSize -> 100, 
    stringOrderType ->SimilarityEncoder.frequencyDesc, 
    handleInvalid -> SimilarityEncoder.KEEP_INVALID)


  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
    require(inputDataType == StringType,
      s"The input column $inputColName must be a string" +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = $(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")

    val outputFields = inputFields :+ new StructField(outputColName, VectorType, true)

    StructType(outputFields)
  }

  protected def getCategories(dataset: Dataset[_]): Array[(String, Int)] = {
    val inputColName = $(inputCol)

  val labels= (dataset
    .select(col($(inputCol)).cast(StringType))
    .na.drop(Array($(inputCol)))
    .groupBy($(inputCol))
    .count())

    // Different options to sort
    val vocabulary: Array[Row] = $(stringOrderType) match {
      case SimilarityEncoder.frequencyDesc  => labels.sort(col("count").desc).take($(vocabSize))
      case SimilarityEncoder.frequencyAsc => labels.sort(col("count")).take($(vocabSize))
    }

  vocabulary.map(row => (row.getAs[String](0), row.getAs[Int](1))).toArray

  }
}


class SimilarityEncoder private[dirty_cat] (override val uid: String) extends Estimator[SimilarityEncoderModel] with SimilarityBase { 

  def this() = this(Identifiable.randomUID("SimilarityEncoder"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setNGramSize(value: Int): this.type = set(nGramSize, value)
  
  def setVocabSize(value: Int): this.type = set(vocabSize, value)
  
  def setSimilarityType(value: String): this.type = set(similarityType, value)
  
  def setStringOrderType(value: String): this.type = set(stringOrderType, value)
  
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)


  override def copy(extra: ParamMap): SimilarityEncoder = { defaultCopy(extra) }   


  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
}

  override def fit(dataset: Dataset[_]): SimilarityEncoderModel = {

    transformSchema(dataset.schema, logging = true)
    
    val vocabularyReference =  getCategories(dataset).take($(vocabSize))

    copyValues(new SimilarityEncoderModel(uid, 
      vocabularyReference.toMap).setParent(this))

  }
}


object SimilarityEncoder extends DefaultParamsReadable[SimilarityEncoder] {
  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val KEEP_INVALID: String = "keep"
  private[feature] val supportedHandleInvalids: Array[String] =
    Array(SKIP_INVALID, KEEP_INVALID)
  private[feature] val frequencyDesc: String = "frequencyDesc"
  private[feature] val frequencyAsc: String = "frequencyAsc"
  private[feature] val supportedStringOrderType: Array[String] =
    Array(frequencyDesc, frequencyAsc)
  private[feature] val nGram: String = "nGram"
  private[feature] val leverstein: String = "leverstein"
  private[feature] val jako: String = "jako"
  private[feature] val supportedSimilarityType: Array[String] = Array(nGram, leverstein, jako)

  override def load(path: String): SimilarityEncoder = super.load(path)
}




/* This encoding is an alternative to OneHotEncoder in the case of
dirty categorical variables. */
class SimilarityEncoderModel private[dirty_cat] (override val uid: String, 
  val vocabularyReference: Map[String, Int]) extends
 Model[SimilarityEncoderModel] with SimilarityBase with MLWritable with Serializable{

  import SimilarityEncoderModel._

  // only called in copy()
  def this(uid: String) = this(uid, null)


  private def int2Integer(x: Int) = java.lang.Integer.valueOf(x)

  private def string2String(x: String) = java.lang.String.valueOf(x)

  /* Java-friendly version of [[vocabularyReference]] */
  def javaVocabularyReference: JMap[JString, JInt] = {
    vocabularyReference.map{ case (k, v) => string2String(k) -> int2Integer(v) }.asJava}


  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setNGramSize(value: Int): this.type = set(nGramSize, value)

  def setSimilarityType(value: String): this.type = set(similarityType, value)
  

  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains($(inputCol))) {
      validateAndTransformSchema(schema)
    } else {
      // If the input column does not exist during transformation, we skip
      // SimilarityEncoderModel.
      schema
    }
  }


  override def transform(dataset: Dataset[_]): DataFrame = {

    if (!dataset.schema.fieldNames.contains($(inputCol))) {
      logInfo(s"Input column ${$(inputCol)} does not exist during transformation. " +
        "Skip SimilarityEncoderModel.")
      return dataset.toDF
    }

    transformSchema(dataset.schema, logging = true)
    
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)
    // Transformation
    val vocabulary =  getCategories(dataset).toArray
    val vocabularyLabels = vocabulary.map(_._1).toSeq
    val vocabularyReferenceLabels = vocabularyReference.toArray.map(_._1)


    val similarityValues = $(similarityType) match {
      case SimilarityEncoder.nGram => StringSimilarity.getNGramSimilaritySeq(vocabularyLabels, vocabularyReferenceLabels, $(nGramSize))
      case SimilarityEncoder.leverstein => StringSimilarity.getLevenshteinSimilaritySeq(vocabularyLabels, vocabularyReferenceLabels)
      case SimilarityEncoder.jako => StringSimilarity.getJaroWinklerSimilaritySeq(vocabularyLabels, vocabularyReferenceLabels)
    }

     val labelToEncode: OpenHashMap[String, Array[Double]] = {
     val n = vocabularyLabels.length
     val map = new OpenHashMap[String, Array[Double]](n)
     var i = 0
     while (i < n ){
       map.update(vocabularyLabels(i), similarityValues(i).toArray)
       i += 1   
      }
      map
    }

    val (filteredDataset, keepInvalid) = $(handleInvalid) match {
          case SimilarityEncoder.SKIP_INVALID =>
            val filterer = udf { label: String =>
              labelToEncode.contains(label)
            }
            (dataset.na.drop(Array($(inputCol))).where(filterer(dataset($(inputCol)))), false)
          case _ => (dataset, getHandleInvalid == SimilarityEncoder.KEEP_INVALID)
    }

    val emptyValues = Vectors.dense(Array.fill($(vocabSize))(0D))

    val dirtyCatUDF = udf { label: String =>
      if (label == null) {
        if (keepInvalid) {
          emptyValues 
        } else {
          throw (new SparkException("SimilarityEncoder encountered NULL value. To handle or skip " +
            "NULLS, try setting SimilarityEncoder.handleInvalid."))
        }
      } else {
        if (labelToEncode.contains(label)) {
          Vectors.dense(labelToEncode(label))
        } else if (keepInvalid) {
          emptyValues 
        } else {
          throw (new SparkException(s"Unseen label: $label.  To handle unseen labels, " +
            s"set Param handleInvalid to ${SimilarityEncoder.KEEP_INVALID}."))
        }
      }
    }.asNondeterministic()

    filteredDataset.withColumn($(outputCol), dirtyCatUDF(col($(inputCol))))
  }

   override def copy(extra: ParamMap) = { defaultCopy(extra) }


   override def write: MLWriter = new SimilarityEncoderModelWriter(this)

}


/// Add read and write
/** [[MLWriter]] instance for [[SimilarityEncoderModel]] */
object SimilarityEncoderModel extends MLReadable[SimilarityEncoderModel] {
  //
  override def read: MLReader[SimilarityEncoderModel] = new SimilarityEncoderModelReader

  override def load(path: String): SimilarityEncoderModel = super.load(path)

  private[SimilarityEncoderModel]
  class SimilarityEncoderModelWriter(instance: SimilarityEncoderModel) extends MLWriter {

    private case class Data(vocabularyReference: Map[String, Int])

    override protected def saveImpl(path: String): Unit = {

      implicit val sparkSession = super.sparkSession
      implicit val sc = sparkSession.sparkContext
      
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data
      val data = Data(instance.vocabularyReference)
      val dataPath = new Path(path, "data").toString

      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }


 private class SimilarityEncoderModelReader extends MLReader[SimilarityEncoderModel] {

     private val className = classOf[SimilarityEncoderModel].getName

     override def load(path: String): SimilarityEncoderModel = {

       implicit val sc = super.sparkSession.sparkContext

       val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

       val dataPath = new Path(path, "data").toString
       val data = sparkSession.read.parquet(dataPath).select("vocabularyReference")

       val vocabularyReference = data.head().getAs[Map[String, Int]](0)
      val model = new SimilarityEncoderModel(metadata.uid, vocabularyReference)  
      DefaultParamsReader.getAndSetParams(model, metadata)
      
      model
     }
 }


}




