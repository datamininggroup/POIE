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

package object neural {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "model.neural."
  //////////////////////////////////////////////////////////////////
  
  ////  simpleLayers (SimpleLayers)
  class SimpleLayers(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "simpleLayers"
    def sig = Sig(List(
      "datum" -> P.Array(P.Double),
      "model" -> P.Array(P.WildRecord("M", ListMap(
        "weights" -> P.Array(P.Array(P.Double)),
        "bias" ->    P.Array(P.Double)
      ))),
      "activation" -> P.Fcn(List(P.Double), P.Double)), P.Array(P.Double))
    def doc =
      <doc>
        <desc>Apply a feedforward artificial neural network <p>model</p> to an input <p>datum</p>.</desc>
        <param name="datum">Length <p>d</p> vector of independent variables.</param>
        <param name="model">Array containing the parameters of each layer of the feedforward neural network model.</param>
          <paramField name="weights">Matrix of weights.  Each of <p>i</p> rows is a node (or neuron), and in each of <p>j</p> columns are the weights for that node.  In the zeroth layer, <p>i</p> must equal <p>d</p>.</paramField>
          <paramField name="bias">Length <p>j</p> vector of biases that are added to each node output.</paramField>
        <param name="activation">Function applied at the output of each node, except the last.  Usually an "S"-shaped sigmoid or hyperbolic tangent.</param>
        <ret>Returns an array of network outputs.  For a neural network with a single neuron in the last layer (single output), this is an array of length one.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises a "no layers" error if the length of model is zero.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "weights, bias, or datum misaligned" error if there is any misalignment between inputs and outputs through the layers of the network.</error>
      </doc>
    def errcodeBase = 11000

    def matrix_vector_mult(a: Vector[PFAArray[Double]], b: Vector[Double]): Vector[Double] =
      for (row <- a) yield row.toVector zip b map Function.tupled(_ * _) sum

    def apply(datum: PFAArray[Double], model: PFAArray[PFARecord], activation: (Double => Double)): PFAArray[Double] = {
      val vector = model.toVector
      val length = vector.size
      if (length == 0)
        throw new PFARuntimeException("no layers", errcodeBase + 0, name, pos)
      var x = datum.toVector
      var i = 0
      // pass datum through first N - 1 layers, apply activation function
      while (i < (length - 1)) {
        val thisLayer = vector(i)
        val bias    = thisLayer("bias").asInstanceOf[PFAArray[Double]].toVector
        val weights = thisLayer("weights").asInstanceOf[PFAArray[PFAArray[Double]]].toVector
        if (bias.size != weights.size || weights.exists(_.toVector.size != datum.size))
          throw new PFARuntimeException("weights, bias, or datum misaligned", errcodeBase + 1, name, pos)
        x = (matrix_vector_mult(weights, x), bias).zipped.map(_+_).map(activation(_))
        i += 1
      }
      // pass through final layer, don't apply activation function
      val thisLayer = vector(i)
      val bias    = thisLayer("bias").asInstanceOf[PFAArray[Double]].toVector
      val weights = thisLayer("weights").asInstanceOf[PFAArray[PFAArray[Double]]].toVector
      if (bias.size != weights.size || weights.exists(_.toVector.size != x.size))
        throw new PFARuntimeException("weights, bias, or datum misaligned", errcodeBase + 1, name, pos)
      PFAArray.fromVector((matrix_vector_mult(weights, x), bias).zipped.map(_+_))
    }
  }
  provide(new SimpleLayers)
}
