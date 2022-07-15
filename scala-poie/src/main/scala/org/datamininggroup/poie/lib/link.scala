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

package org.datamininggroup.poie.lib

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


package object link {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "m.link."

  //////////////////////////////////////////////////////////////////// S-shaped functions
  ////   softmax (SoftMax)
  class SoftMax(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "softmax"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Map(P.Double)), P.Map(P.Double))))
    def doc =
      <doc>
        <desc>Normalize a prediction with the softmax function.</desc>
        <ret>Each element <m>x_i</m> is mapped to <m>\exp(x_i)/\sum_j \exp(x_j)</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>If <p>x</p> is an empty array or an empty map, this function raises an "empty input" error.</error>
      </doc>
    def errcodeBase = 25000
    def apply(x: PFAArray[Double]): PFAArray[Double] = {
      var xx = x.toVector
      if (xx.isEmpty)
        throw new PFARuntimeException("empty input", errcodeBase + 0, name, pos)
      if (Math.abs(xx.max) >= Math.abs(xx.min)) {
        xx = xx.map(y => y - xx.max) 
      } else {
        xx = xx.map(y => y - xx.min) 
      }
      val denom = xx.fold(0.0) {_ + Math.exp((_))}
      PFAArray.fromVector(xx map {Math.exp(_) / denom})
    }
    def apply(x: PFAMap[java.lang.Double]): PFAMap[java.lang.Double] = {
      var xx = x.toMap map {case (k, v) => (k, v.doubleValue)}
      if (xx.isEmpty)
        throw new PFARuntimeException("empty input", errcodeBase + 0, name, pos)
      if (Math.abs(xx.values.max) >= Math.abs(xx.values.min)) {
        xx = xx.map { case (k,v)  => (k, v.doubleValue - xx.values.max) }
      } else {
        xx = xx.map { case (k,v)  => (k, v.doubleValue - xx.values.min) }
      }
      val denom = xx.values.fold(0.0) {_ + Math.exp(_)}
      PFAMap.fromMap(xx map {case (k, v) => (k, java.lang.Double.valueOf(Math.exp(v) / denom))})
    }
  }
  provide(new SoftMax)

  trait UnwrapForNorm {
    def apply(x: Double): Double

    def apply(x: PFAArray[Double]): PFAArray[Double] =
      PFAArray.fromVector(x.toVector.map(apply(_)))

    def apply(x: PFAMap[java.lang.Double]): PFAMap[java.lang.Double] =
      PFAMap.fromMap(x.toMap map {case (k, v) => (k, java.lang.Double.valueOf(apply(v.doubleValue)))})
  }

  ////   logit (Logit)
  class Logit(val pos: Option[String] = None) extends LibFcn with UnwrapForNorm {
    def name = prefix + "logit"
    def sig = Sigs(List(Sig(List("x" -> P.Double), P.Double),
                        Sig(List("x" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Map(P.Double)), P.Map(P.Double))))
    def doc =
      <doc>
        <desc>Normalize a prediction with the logit function.</desc>
        <ret>Each element <m>x_i</m> is mapped to <m>1 / (1 + \exp(-x_i))</m>.</ret>
      </doc>
    def errcodeBase = 25010
    def apply(x: Double): Double = 1.0 / (1.0 + Math.exp(-x))
  }
  provide(new Logit)

  ////   probit (Probit)
  class Probit(val pos: Option[String] = None) extends LibFcn with UnwrapForNorm {
    def name = prefix + "probit"
    def sig = Sigs(List(Sig(List("x" -> P.Double), P.Double),
                        Sig(List("x" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Map(P.Double)), P.Map(P.Double))))
    def doc =
      <doc>
        <desc>Normalize a prediction with the probit function.</desc>
        <ret>Each element <m>x_i</m> is mapped to <m>{"""(\mbox{erf}(x_i/\sqrt{2}) + 1)/2"""}</m>.</ret>
      </doc>
    def errcodeBase = 25020
    def apply(x: Double): Double = (special.Erf.erf(x/Math.sqrt(2.0)) + 1.0)/2.0
  }
  provide(new Probit)

  ////   cloglog (CLogLog)
  class CLogLog(val pos: Option[String] = None) extends LibFcn with UnwrapForNorm {
    def name = prefix + "cloglog"
    def sig = Sigs(List(Sig(List("x" -> P.Double), P.Double),
                        Sig(List("x" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Map(P.Double)), P.Map(P.Double))))
    def doc =
      <doc>
        <desc>Normalize a prediction with the cloglog function.</desc>
        <ret>Each element <m>x_i</m> is mapped to <m>1 - \exp(-\exp(x_i))</m>.</ret>
      </doc>
    def errcodeBase = 25030
    def apply(x: Double): Double = 1.0 - Math.exp(-Math.exp(x))
  }
  provide(new CLogLog)

  ////   loglog (LogLog)
  class LogLog(val pos: Option[String] = None) extends LibFcn with UnwrapForNorm {
    def name = prefix + "loglog"
    def sig = Sigs(List(Sig(List("x" -> P.Double), P.Double),
                        Sig(List("x" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Map(P.Double)), P.Map(P.Double))))
    def doc =
      <doc>
        <desc>Normalize a prediction with the loglog function.</desc>
        <ret>Each element <m>x_i</m> is mapped to <m>\exp(-\exp(x_i))</m>.</ret>
      </doc>
    def errcodeBase = 25040
    def apply(x: Double): Double = Math.exp(-Math.exp(x))
  }
  provide(new LogLog)

  ////   cauchit (Cauchit)
  class Cauchit(val pos: Option[String] = None) extends LibFcn with UnwrapForNorm {
    def name = prefix + "cauchit"
    def sig = Sigs(List(Sig(List("x" -> P.Double), P.Double),
                        Sig(List("x" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Map(P.Double)), P.Map(P.Double))))
    def doc =
      <doc>
        <desc>Normalize a prediction with the cauchit function.</desc>
        <ret>Each element <m>x_i</m> is mapped to <m>{"""0.5 + (1/\pi) \tan^{-1}(x_i)"""}</m>.</ret>
      </doc>
    def errcodeBase = 25050
    def apply(x: Double): Double = 0.5 + (1.0/Math.PI) * Math.atan(x)
  }
  provide(new Cauchit)

  ////   softplus (Softplus)
  class Softplus(val pos: Option[String] = None) extends LibFcn with UnwrapForNorm {
    def name = prefix + "softplus"
    def sig = Sigs(List(Sig(List("x" -> P.Double), P.Double),
                        Sig(List("x" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Map(P.Double)), P.Map(P.Double))))
    def doc =
      <doc>
        <desc>Normalize a prediction with the softplus function.</desc>
        <ret>Each element <m>x_i</m> is mapped to <m>{"""\log(1.0 + \exp(x_i))"""}</m>.</ret>
      </doc>
    def errcodeBase = 25060
    def apply(x: Double): Double = Math.log(1.0 + Math.exp(x))
  }
  provide(new Softplus)

  ////   relu (Relu)
  class Relu(val pos: Option[String] = None) extends LibFcn with UnwrapForNorm {
    def name = prefix + "relu"
    def sig = Sigs(List(Sig(List("x" -> P.Double), P.Double),
                        Sig(List("x" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Map(P.Double)), P.Map(P.Double))))
    def doc =
      <doc>
        <desc>Normalize a prediction with the rectified linear unit (ReLu) function.</desc>
        <ret>Each element <m>x_i</m> is mapped to <m>{"""\log(1.0 + \exp(x_i))"""}</m>.</ret>
      </doc>
    def errcodeBase = 25070
    def apply(x: Double): Double = Math.max(0, x)
  }
  provide(new Relu)

  ////   tanh (Tanh)
  class Tanh(val pos: Option[String] = None) extends LibFcn with UnwrapForNorm {
    def name = prefix + "tanh"
    def sig = Sigs(List(Sig(List("x" -> P.Double), P.Double),
                        Sig(List("x" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Map(P.Double)), P.Map(P.Double))))
    def doc =
      <doc>
        <desc>Normalize a prediction with the hyperbolic tangent function.</desc>
        <ret>Each element <m>x_i</m> is mapped to <m>{"""\tanh(x_i)"""}</m>.</ret>
      </doc>
    def errcodeBase = 25080
    def apply(x: Double): Double = Math.tanh(x)
  }
  provide(new Tanh)
}
