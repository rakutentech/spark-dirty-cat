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


import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.SparkException
import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.apache.spark.sql.types.{StringType, IntegerType, StructType, StructField}
import org.apache.spark.sql.Row



class SimilarityEncoderSuite extends FunSuite with SharedSparkContext {

  import com.rakuten.dirty_cat.feature.{SimilarityEncoder, SimilarityEncoderModel}


  private def generateDataFrame(): DataFrame = {
    
    val schema = StructType(List(StructField("id", IntegerType),
      StructField("name", StringType)))
    
    val sqlContext = new SQLContext(sc)

    val rdd = sc.parallelize(Seq(
       Row(0, "andres"),
       Row(1, "andrea"),
       Row(2, "carlos I"),
       Row(3, "camilo II"),
       Row(4, "camila de aragon"),
       Row(5, "guido"),
       Row(6, "camilo II"),    
       Row(7, "guido"),
       Row(8, "guido"),
       Row(9, "andrea"),
       Row(10, "andrea"), 
       Row(11, "camila de aragon")))

   val dataframe = sqlContext.createDataFrame(rdd, schema)

   dataframe

  }


 test("coverage") {

   List("leverstein", "nGram", "jako").map{similarity => 
     
     val encoder = (new SimilarityEncoder()
       .setInputCol("name")
       .setOutputCol("nameEncoded")
       .setSimilarityType(similarity))
   }
 }



test("fit"){

   val dataframe = generateDataFrame()

   val encoder = (new SimilarityEncoder()
     .setInputCol("name")
     .setOutputCol("nameEncoded")
     .setSimilarityType("nGram"))

  val encoderModel = encoder.fit(dataframe)

  val dataframeEncoded = encoderModel.transform(dataframe)


  // intercept[SparkException] {
  //    val encoder = (new SimilarityEncoder()
  //      .setInputCol("name")
  //      .setOutputCol("nameEncoded")
  //      .setSimilarityType("XXX"))
  // }


  }


  test("pipeline"){

    import org.apache.spark.ml.{Pipeline, PipelineModel}
    import org.apache.spark.ml.feature.StandardScaler

   val dataframe = generateDataFrame()

   val encoder = (new SimilarityEncoder()
     .setInputCol("name")
     .setOutputCol("nameEncoded")
     .setSimilarityType("nGram"))

   val scaler = (new StandardScaler()
     .setInputCol("nameEncoded")
     .setOutputCol("scaledFeatures"))

   val pipeline = (new Pipeline()
     .setStages(Array(encoder, scaler)))

   val pipelineModel = pipeline.fit(dataframe)

   val dataframeFeatures = pipelineModel.transform(dataframe)

  }

// test("SimilarityEncoderNulls") {}


// test("SimilarityEncoderUnseen") {}
//
//  test("SimilarityEncoderModel read/write") {}
}


