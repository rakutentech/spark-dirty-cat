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

package com.rakuten.dirty_cat.utils


import org.apache.spark.sql.functions.udf

package object TextUtils {

  import java.text.Normalizer
  import scala.util.Try

  def normalizeString(input: String): String = {
      Try{
          val cleaned = input.trim.toLowerCase
          val normalized = (Normalizer.normalize(cleaned, Normalizer.Form.NFD)
                            .replaceAll("[\\p{InCombiningDiacriticalMarks}\\p{IsM}\\p{IsLm}\\p{IsSk}]+", ""))       
          (normalized
           .replaceAll("'s", "")
           .replaceAll("ß", "ss")
           .replaceAll("ø", "o")
           .replaceAll("[^a-zA-Z0-9-]+", "-")
           .replaceAll("-+", "-")
           .stripSuffix("-"))
      }.getOrElse(input) 
  }

  val normalizeStringUDF = udf[String, String](normalizeString(_)) 
}

