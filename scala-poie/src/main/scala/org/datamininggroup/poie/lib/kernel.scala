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


package object kernel {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "m.kernel."

  //////////////////////////////////////////////////////////////////
  def dot(a: Vector[Double], b: Vector[Double], code: Int, fcnName: String, pos: Option[String]): Double = {
    if (a.size != b.size)
      throw new PFARuntimeException("arrays must have same length", code, fcnName, pos)
    (for ((i,j) <- a zip b) yield i * j) sum 
  }
  //////////////////////////////////////////////////////////////////
  ////  linear (Linear)
  class Linear(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "linear"
    def sig = Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double)), P.Double) 
    def doc =
      <doc>
        <desc>Linear kernel function.</desc>
        <param name="x">Length <p>n</p> vector.</param>
        <param name="y">Length <p>n</p> vector.</param>
        <ret>Returns the dot product of <p>x</p> and <p>y</p>, <m>{"\\sum_{i=1}^{n} x_{i} y_{j}"}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "arrays must have same length" error if the lengths of <p>x</p> and <p>y</p> are not the same.</error>
      </doc>
    def errcodeBase = 23000
    
    def apply(x: PFAArray[Double], y: PFAArray[Double]): Double = {
      dot(x.toVector, y.toVector, errcodeBase + 0, name, pos)
    }
  }
  provide(new Linear)

  ////  rbf (RBF)
  class RBF(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "rbf"
    def sig = Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double), "gamma" -> P.Double), P.Double) 
    def doc =
      <doc>
        <desc>Radial Basis Function (RBF) kernel function.</desc>
        <param name="x">Length <p>n</p> vector.</param>
        <param name="y">Length <p>n</p> vector.</param>
        <param name="gamma">Gamma coefficient.</param>
        <ret>Returns the result of <m>{"\\mathrm{exp}(-\\gamma || x - y ||^{2})"}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "arrays must have same length" error if the lengths of <p>x</p> and <p>y</p> are not the same.</error>
      </doc>
    def errcodeBase = 23010
    
    def apply(x: PFAArray[Double], y: PFAArray[Double], gamma: Double): Double = {
      if (x.size != y.size)
        throw new PFARuntimeException("arrays must have same length", errcodeBase + 0, name, pos)
      var out = 0.0
      for ((i,j) <- (x.toVector zip y.toVector)) 
        out += Math.pow(Math.abs(i - j), 2)
      Math.exp(-gamma*out)
    }
  }
  provide(new RBF)

  ////  poly (Poly)
  class Poly(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "poly"
    def sig = Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double), "gamma" -> P.Double, "intercept" -> P.Double, "degree" -> P.Double), P.Double) 
    def doc =
      <doc>
        <desc>Polynomial kernel function.</desc>
        <param name="x">Length <p>n</p> vector.</param>
        <param name="y">Length <p>n</p> vector.</param>
        <param name="gamma">Gamma coefficient.</param>
        <param name="intecept">Intercept constant.</param>
        <param name="degree">Degree of the polynomial kernel.</param>
        <ret>Returns the result of <m>{"(\\gamma \\sum_{i=1}^{n} x_{i} y_{j} + \\mathrm{intercept})^{\\mathrm{degree}}"}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "arrays must have same length" error if the lengths of <p>x</p> and <p>y</p> are not the same.</error>
      </doc>
    def errcodeBase = 23020
    
    def apply(x: PFAArray[Double], y: PFAArray[Double], gamma: Double, intercept: Double, degree: Double): Double = {
      Math.pow(gamma*dot(x.toVector, y.toVector, errcodeBase + 0, name, pos) + intercept, degree) 
    }
  }
  provide(new Poly)
  
  ////  sigmoid (Sigmoid)
  class Sigmoid(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "sigmoid"
    def sig = Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double), "gamma" -> P.Double, "intercept" -> P.Double), P.Double) 
    def doc =
      <doc>
        <desc>Sigmoid kernel function.</desc>
        <param name="x">Length <p>n</p> vector.</param>
        <param name="y">Length <p>n</p> vector.</param>
        <param name="gamma">Gamma coefficient.</param>
        <param name="intecept">Intercept constant.</param>
        <ret>Returns the result of <m>{"\\mathrm{tanh}( \\mathrm{gamma} \\sum_{i=1}^{n} x_{i} y_{j} + \\mathrm{intercept})"}</m>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "arrays must have same length" error if the lengths of <p>x</p> and <p>y</p> are not the same.</error>
      </doc>
    def errcodeBase = 23030
    
    def apply(x: PFAArray[Double], y: PFAArray[Double], gamma: Double, intercept: Double): Double = {
      Math.tanh(gamma * dot(x.toVector, y.toVector, errcodeBase + 0, name, pos) + intercept) 
    }
  }
  provide(new Sigmoid)
}
