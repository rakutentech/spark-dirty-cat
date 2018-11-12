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

package com.rakuten.dirty_cat.util


import scala.collection.mutable
import scala.collection.parallel.ParSeq
import org.apache.commons.lang3.StringUtils


private[dirty_cat] object StringSimilarity{


  private[util] def getNGrams(string: String, n: Int): List[List[String]] = {
      // start = <s> and end = </s>
      val tokens = List("<s>") ::: string.toLowerCase().split("").toList ::: List("</s>")
      val nGram = tokens.sliding(n).toList
      nGram }


  private[util] def getCounts(nGram: List[List[String]]): Map[List[String],Int] = {
      nGram.groupBy(identity).mapValues(_.size) }


  private[util] def getNGramSimilarity(string1: String, string2: String, n: Int): Double = {
      val ngrams1 = getNGrams(string1, n)
      val ngrams2 = getNGrams(string2, n)
      val counts1 = getCounts(ngrams1)
      val counts2 = getCounts(ngrams2)

      val sameGrams = (counts1.keySet.intersect(counts2.keySet)
          .map(k => k -> List(counts1(k), counts2(k))).toMap)

      val nSameGrams = sameGrams.size
      val nAllGrams = ngrams1.length + ngrams2.length

      val similarity = nSameGrams.toDouble / (nAllGrams.toDouble - nSameGrams.toDouble)

      similarity }

   
  def getLevenshteinRatio(string1: String, string2: String): Double = {
      val totalLength = (string1.length + string2.length).toDouble
      if (totalLength == 0D){ 1D } else { (totalLength - StringUtils.getLevenshteinDistance(string1, string2)) / totalLength }}  


  def getJaroWinklerRatio(string1: String, string2: String): Double = {
      val totalLength = (string1.length + string2.length).toDouble
      if (totalLength == 0D){ 1D } else { (totalLength - StringUtils.getJaroWinklerDistance(string1, string2)) / totalLength }}  


  def getNGramSimilaritySeq(data: Seq[String], categories: Seq[String], n: Int): Seq[Seq[Double]] = {
      data.map{xi => categories.map{yi => getNGramSimilarity(xi, yi, n)}}}


  def getLevenshteinSimilaritySeq(data: Seq[String], categories: Seq[String]): Seq[Seq[Double]] = {
      data.map{xi => categories.map{yi => getLevenshteinRatio(xi, yi)}}}


  def getJaroWinklerSimilaritySeq(data: Seq[String], categories: Seq[String]): Seq[Seq[Double]] = {
      data.map{xi => categories.map{yi => getJaroWinklerRatio(xi, yi)}}}
}

