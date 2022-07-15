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
import scala.collection.mutable
import scala.util.Random

import org.apache.avro.AvroRuntimeException

import com.thesamet.spatial.DimensionalOrdering
import com.thesamet.spatial.Metric
import com.thesamet.spatial.KDTree

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

package object neighbor {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "model.neighbor."

  //////////////////////////////////////////////////////////////////// 

  ////   mean (Mean)
  class Mean(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "mean"
    def sig = Sigs(List(Sig(List("points" -> P.Array(P.Array(P.Double))), P.Array(P.Double)),
                        Sig(List("points" -> P.Array(P.Array(P.Double)), "weight" -> P.Fcn(List(P.Array(P.Double)), P.Double)), P.Array(P.Double))))
    def doc =
      <doc>
        <desc>Return the vector-wise mean of <p>points</p>, possibly weighted by <p>weight</p>.</desc>
        <param name="points">Points from a codebook, for instance from <f>model.neighbor.nearestK</f>.</param>
        <param name="weight">Optional weighting function from each element of <p>points</p> to a value.  If these values do not add up to 1.0, they will be internally normalized.</param>
        <ret>The vector-wise mean, which is by construction within the convex hull of the <p>points</p>.</ret>
        <error code={s"${errcodeBase + 0}"}>If <p>points</p> is empty, a "not enough points" error will be raised.</error>
        <error code={s"${errcodeBase + 1}"}>If the <p>points</p> have different sizes, an "inconsistent dimensionality" error will be raised.</error>
      </doc>
    def errcodeBase = 30000
    def apply(points: PFAArray[PFAArray[Double]], weight: PFAArray[Double] => Double): PFAArray[Double] = {
      val vector = points.toVector
      if (vector.isEmpty)
        throw new PFARuntimeException("not enough points", errcodeBase + 0, name, pos)

      val dimensions = vector.head.size
      val numer = Array.fill[Double](dimensions)(0.0)
      val denom = Array.fill[Double](dimensions)(0.0)
      vector foreach {point =>
        val p = point.toVector
        if (p.size != dimensions)
          throw new PFARuntimeException("inconsistent dimensionality", errcodeBase + 1, name, pos)

        val w = if (weight == null) 1.0 else weight(point)

        for (i <- 0 until dimensions) {
          numer(i) += w * p(i)
          denom(i) += w
        }
      }
      for (i <- 0 until dimensions)
        numer(i) /= denom(i)

      PFAArray.fromVector(numer.toVector)
    }
    def apply(points: PFAArray[PFAArray[Double]]): PFAArray[Double] = apply(points, null)
  }
  provide(new Mean)

  ////   nearestK (NearestK)
  object NearestK {
    private val squaredEuclidean = {(x: PFAArray[Double], y: PFAArray[Double]) => (x.toVector zip y.toVector) map {case (x, y) => (x - y)*(x - y)} sum}

    private class PFAArrayDimensionalOrdering(val dimensions: Int) extends DimensionalOrdering[PFAArray[Double]] {
      def compareProjection(d: Int)(x: PFAArray[Double], y: PFAArray[Double]) = {
        val xv = x.toVector
        val yv = y.toVector
        xv(d) compareTo yv(d)
      }
    }

    private implicit val pfaArrayMetric = new Metric[PFAArray[Double], Double] {
      def distance(x: PFAArray[Double], y: PFAArray[Double]): Double = squaredEuclidean(x, y)
      def planarDistance(d: Int)(x: PFAArray[Double], y: PFAArray[Double]): Double = {
        val xv = x.toVector
        val yv = y.toVector
        val diff = (xv(d) - yv(d))
        diff * diff
      }
    }

    private val minCountForTreeSearch = 10
    private def treeSearch(k: Int, datum: PFAArray[Double], kdtree: KDTree[PFAArray[Double]]): PFAArray[PFAArray[Double]] =
      PFAArray.fromVector(kdtree.findNearest(datum, k).toVector)

  }
  class NearestK(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "nearestK"
    def sig = Sigs(List(
      Sig(List(
        "k" -> P.Int,
        "datum" -> P.Array(P.Double),
        "codebook" -> P.Array(P.Array(P.Double))
      ), P.Array(P.Array(P.Double))),
      Sig(List(
        "k" -> P.Int,
        "datum" -> P.Wildcard("A"),
        "codebook" -> P.Array(P.Wildcard("B")),
        "metric" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B")), P.Double)
      ), P.Array(P.Wildcard("B")))))

    def doc =
      <doc>
        <desc>Find the <p>k</p> items in the <p>codebook</p> that are closest to the <p>datum</p>, according to the <p>metric</p>.</desc>
        <param name="k">Number of <p>codebook</p> points to attempt to return.</param>
        <param name="datum">Sample datum.</param>
        <param name="codebook">Set of training data that is compared to the <p>datum</p>.</param>
        <param name="metric">Function used to compare each <p>datum</p> to each element of the <p>codebook</p>.  (See, for example, <f>metric.euclidean</f>.)</param>
        <ret>An array of the closest <p>codebook</p> elements in any order.  The length of the array is minimum of <p>k</p> and the length of <p>codebook</p>.</ret>
        <error code={s"${errcodeBase + 0}"}>If <p>k</p> is negative, an "k must be nonnegative" error will be raised.</error>
        <error code={s"${errcodeBase + 1}"}>If arrays in the <p>codebook</p> or the <p>codebook</p> and the <p>datum</p> have different sizes (without a <p>metric</p>), an "inconsistent dimensionality" error will be raised.</error>
      </doc>
    def errcodeBase = 30010

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      if (engineOptions.lib_model_neighbor_nearestK_kdtree)
        JavaCode("%s.applyKDTree(%s)", javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString, wrapArgs(args, paramTypes, true))
      else
        JavaCode("%s.apply(%s)", javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString, wrapArgs(args, paramTypes, true))

    import NearestK._

    def apply[A, B](k: Int, datum: A, codebook: PFAArray[B], metric: (A, B) => Double): PFAArray[B] = {
      if (k < 0)
        throw new PFARuntimeException("k must be nonnegative", errcodeBase + 0, name, pos)

      val vector = codebook.toVector
      val distances = vector.map(metric(datum, _)) toArray

      // this will automatically sort them, but we don't want to restrict this function to always return sorted items for the sake of future optimizations
      var indexes = List[Int]()
      var indexesSize = 0
      var i = 0
      while (i < distances.size) {
        val (before, after) = indexes.span(besti => distances(i) < distances(besti))
        if (before.isEmpty) {
          if (indexesSize < k) {
            indexes = i :: after
            indexesSize += 1
          }
        }
        else {
          indexes = before ++ (i :: after)
          if (indexesSize >= k)
            indexes = indexes.tail
          else
            indexesSize += 1
        }
        i += 1
      }

      PFAArray.fromVector(indexes.reverse.toVector map {i => vector(i)})
    }

    def apply(k: Int, datum: PFAArray[Double], codebook: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] = {
      if (k < 0)
        throw new PFARuntimeException("k must be nonnegative", errcodeBase + 0, name, pos)

      val vector = codebook.toVector
      if (vector.isEmpty)
        return PFAArray.empty[PFAArray[Double]](0)
      val dimensions = datum.size
      vector foreach {x => if (x.size != dimensions) throw new PFARuntimeException("inconsistent dimensionality", errcodeBase + 1, name, pos)}

      apply(k, datum, codebook, squaredEuclidean)
    }

    def applyKDTree(k: Int, datum: PFAArray[Double], codebook: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] = {
      if (k < 0)
        throw new PFARuntimeException("k must be nonnegative", errcodeBase + 0, name, pos)

      val vector = codebook.toVector
      if (vector.isEmpty)
        return PFAArray.empty[PFAArray[Double]](0)
      val dimensions = datum.size
      vector foreach {x => if (x.size != dimensions) throw new PFARuntimeException("inconsistent dimensionality", errcodeBase + 1, name, pos)}

      val metadata = codebook.metadata
      metadata.get("kdtree") match {
        case Some(kdtree) =>
          treeSearch(k, datum, kdtree.asInstanceOf[KDTree[PFAArray[Double]]])

        case None =>
          val count = metadata.getOrElse("count", 0).asInstanceOf[Int]
          if (count < minCountForTreeSearch) {
            codebook.metadata = metadata.updated("count", count + 1)
            apply(k, datum, codebook, squaredEuclidean)
          }
          else {
            implicit val pfaArrayDimensionalOrdering = new PFAArrayDimensionalOrdering(dimensions)
            val kdtree = KDTree(vector: _*)
            codebook.metadata = metadata.updated("kdtree", kdtree)
            treeSearch(k, datum, kdtree)
          }
      }
    }
  }
  provide(new NearestK)

  ////   ballR (BallR)
  class BallR(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "ballR"
    def sig = Sigs(List(
      Sig(List(
        "r" -> P.Double,
        "datum" -> P.Array(P.Double),
        "codebook" -> P.Array(P.Array(P.Double))
      ), P.Array(P.Array(P.Double))),
      Sig(List(
        "r" -> P.Double,
        "datum" -> P.Wildcard("A"),
        "codebook" -> P.Array(P.Wildcard("B")),
        "metric" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B")), P.Double)
      ), P.Array(P.Wildcard("B")))))

    def doc =
      <doc>
        <desc>Find the items in <p>codebook</p> that are within <p>r</p> of the <p>datum</p>, according to the <p>metric</p>.</desc>
        <param name="r">Maximum distance (exclusive) of points to return.</param>
        <param name="datum">Sample datum.</param>
        <param name="codebook">Set of training data that is compared to the <p>datum</p>.</param>
        <param name="metric">Function used to compare each <p>datum</p> to each element of the <p>codebook</p>.  (See, for example, <f>metric.euclidean</f>.)</param>
        <ret>An array of the <p>codebook</p> elements within a distance <p>r</p> in any order.  The length of the array could be as low as zero or as high as the length of <p>codebook</p>.</ret>
      </doc>
    def errcodeBase = 30020

    def apply[A, B](r: Double, datum: A, codebook: PFAArray[B], metric: (A, B) => Double): PFAArray[B] =
      PFAArray.fromVector(codebook.toVector filter {point => metric(datum, point) < r})

    // this is where you could apply optimizations (kd-tree, R*-tree, etc.)
    def apply(r: Double, datum: PFAArray[Double], codebook: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] =
      apply(r, datum, codebook, {(x: PFAArray[Double], y: PFAArray[Double]) => Math.sqrt((x.toVector zip y.toVector) map {case (x, y) => (x - y)*(x - y)} sum)})
  }
  provide(new BallR)

}
