// Copyright 2021 Data Mining Group
//
// This file is part of POIE
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datamininggroup.poie.lib.model

import scala.language.postfixOps
import scala.collection.immutable.ListMap
import scala.util.Random

import org.apache.avro.AvroRuntimeException

import org.ejml.simple.SimpleMatrix
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.commons.math3.special

import org.datamininggroup.poie.ast.LibFcn
import org.datamininggroup.poie.errors.PFARuntimeException
import org.datamininggroup.poie.jvmcompiler.JavaCode
import org.datamininggroup.poie.jvmcompiler.javaSchema
import org.datamininggroup.poie.options.EngineOptions

import org.datamininggroup.poie.ast.AstContext
import org.datamininggroup.poie.ast.ExpressionContext
import org.datamininggroup.poie.ast.FcnDef
import org.datamininggroup.poie.ast.FcnRef

import org.datamininggroup.poie.data.PFAArray
import org.datamininggroup.poie.data.PFAMap
import org.datamininggroup.poie.data.PFARecord
import org.datamininggroup.poie.data.ComparisonOperator

import org.datamininggroup.poie.signature.P
import org.datamininggroup.poie.signature.Sig
import org.datamininggroup.poie.signature.Signature
import org.datamininggroup.poie.signature.Sigs
import org.datamininggroup.poie.signature.Lifespan
import org.datamininggroup.poie.signature.PFAVersion

import org.datamininggroup.poie.datatype.Type
import org.datamininggroup.poie.datatype.FcnType
import org.datamininggroup.poie.datatype.AvroType
import org.datamininggroup.poie.datatype.AvroNull
import org.datamininggroup.poie.datatype.AvroBoolean
import org.datamininggroup.poie.datatype.AvroInt
import org.datamininggroup.poie.datatype.AvroLong
import org.datamininggroup.poie.datatype.AvroFloat
import org.datamininggroup.poie.datatype.AvroDouble
import org.datamininggroup.poie.datatype.AvroBytes
import org.datamininggroup.poie.datatype.AvroFixed
import org.datamininggroup.poie.datatype.AvroString
import org.datamininggroup.poie.datatype.AvroEnum
import org.datamininggroup.poie.datatype.AvroArray
import org.datamininggroup.poie.datatype.AvroMap
import org.datamininggroup.poie.datatype.AvroRecord
import org.datamininggroup.poie.datatype.AvroField
import org.datamininggroup.poie.datatype.AvroUnion

package object svm {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "model.svm."
  //////////////////////////////////////////////////////////////////
  
  ////  score (Score)
  class Score(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "score"
    def sig = Sig(List(
      "datum" -> P.Array(P.Double),
      "model" -> P.WildRecord("L", ListMap( 
        "const" -> P.Double,
        "posClass" -> P.Array(P.WildRecord("M", ListMap(
           "supVec" -> P.Array(P.Double),
           "coeff"  -> P.Double))),
        "negClass" -> P.Array(P.WildRecord("N", ListMap(
           "supVec" -> P.Array(P.Double),
           "coeff"  -> P.Double))))),
      "kernel" -> P.Fcn(List(P.Array(P.Double), P.Array(P.Double)), P.Double)), P.Double)
    def doc =
      <doc>
        <desc>Score an input <p>datum</p> with a two-class support vector machine classifier given a <p>model</p> and a kernel function <p>kernel</p>.</desc>
        <param name="datum">Length <p>d</p> vector of independent variables.</param>
        <param name="model">Record containing the support vectors, dual space coefficients and constant needed to score new data.</param>
          <paramField name="const">Constant learned in training.</paramField>
          <paramField name="posClass">Length <p>i</p> array of records containing the support vectors that are in the region for which the svm returns a score greater than one.</paramField>
            <paramField name="supVec">Length <p>d</p> support vector.</paramField>
            <paramField name="coeff">Dual space coefficient associated with the support vector.</paramField>
          <paramField name="negClass">Length <p>i</p> array of records containing the support vectors that are in the region for which the svm returns a score less than one.</paramField>
            <paramField name="supVec">Length <p>d</p> support vector.</paramField>
            <paramField name="coeff">Dual space coefficient associated with the support vector.</paramField>
        <param name="kernel">Kernel function used to map data and support vectors into the dual space.</param>
        <ret>Returns the score.  If positive, datum classified as same group as <p>posClass</p> support vectors.  If negative, datum classified as same group as <p>negClass</p> support vectors.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "no support vectors" error if the length of <p>negClass</p> and length of <p>posClass</p> is zero.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "support vectors must have same length as datum" error if the length of the support vectors is not the same as the length of <p>datum</p>.</error>
      </doc>
    def errcodeBase = 12000

    def apply(datum: PFAArray[Double], model: PFARecord, kernel: (PFAArray[Double], PFAArray[Double]) => Double): Double = {
      var x        = datum.toVector
      val const    = model("const").asInstanceOf[Double]
      val posClass = model("posClass").asInstanceOf[PFAArray[PFARecord]].toVector 
      val negClass = model("negClass").asInstanceOf[PFAArray[PFARecord]].toVector
      if ((negClass.size == 0) && (posClass.size == 0))
        throw new PFARuntimeException("no support vectors", errcodeBase + 0, name, pos)
      var negClassScore = 0.0
      var supvec = 0.0
      for (sv <- negClass) {
        if (sv.get("supVec").asInstanceOf[PFAArray[Double]].size != datum.size)
          throw new PFARuntimeException("support vectors must have same length as datum", errcodeBase + 1, name, pos)
        negClassScore += kernel(sv.get("supVec").asInstanceOf[PFAArray[Double]], datum) * sv.get("coeff").asInstanceOf[Double]
      }
      var posClassScore = 0.0
      for (sv <- posClass) {
        if (sv.get("supVec").asInstanceOf[PFAArray[Double]].size != datum.size)
          throw new PFARuntimeException("support vectors must have same length as datum", errcodeBase + 1, name, pos)
        posClassScore += kernel(sv.get("supVec").asInstanceOf[PFAArray[Double]], datum) * sv.get("coeff").asInstanceOf[Double]
      }
      posClassScore + negClassScore + const
    }
  }
  provide(new Score)

}
