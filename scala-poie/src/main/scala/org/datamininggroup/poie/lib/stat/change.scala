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

package org.datamininggroup.poie.lib.stat

import scala.annotation.tailrec
import scala.collection.immutable.ListMap

import org.apache.avro.Schema

import org.datamininggroup.poie.ast.LibFcn
import org.datamininggroup.poie.errors.PFARuntimeException
import org.datamininggroup.poie.errors.PFASemanticException
import org.datamininggroup.poie.jvmcompiler.JavaCode
import org.datamininggroup.poie.jvmcompiler.javaSchema
import org.datamininggroup.poie.jvmcompiler.javaType
import org.datamininggroup.poie.jvmcompiler.JVMNameMangle
import org.datamininggroup.poie.jvmcompiler.PFAEngineBase
import org.datamininggroup.poie.options.EngineOptions

import org.datamininggroup.poie.ast.AstContext
import org.datamininggroup.poie.ast.ExpressionContext
import org.datamininggroup.poie.ast.FcnDef
import org.datamininggroup.poie.ast.FcnRef

import org.datamininggroup.poie.data.PFAArray
import org.datamininggroup.poie.data.PFARecord

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

package object change {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "stat.change."

  //////////////////////////////////////////////////////////////////// 

  ////   updateTrigger (UpdateTrigger)
  class UpdateTrigger(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "updateTrigger"
    def sig = Sig(List("predicate" -> P.Boolean, "history" -> P.WildRecord("A", ListMap("numEvents" -> P.Int, "numRuns" -> P.Int, "currentRun" -> P.Int, "longestRun" -> P.Int))), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Update the state of a trigger that counts the number of times <p>predicate</p> is satisfied (<c>true</c>), as well as the number and lengths of runs of <c>true</c>.</desc>
        <param name="predicate">Expression that evaluates to <c>true</c> or <c>false</c>.</param>
        <param name="history">Summary of previous results of the <p>predicate</p>.
          <paramField name="numEvents">The number of times <p>predicate</p> evaluated to <c>true</c>.</paramField>
          <paramField name="numRuns">The number of contiguous intervals in which <p>predicate</p> was <c>true</c>, including the current one.</paramField>
          <paramField name="currentRun">If <p>predicate</p> is <c>false</c>, <pf>currentRun</pf> is 0.  Otherwise, <pf>currentRun</pf> is incremented (greater than or equal to 1 if <p>predicate</p> evaluated to <c>true</c>).</paramField>
          <paramField name="longestRun">The longest run observed so far; may be equal to <pf>currentRun</pf>.</paramField>
        </param>
        <ret>Returns a new record with updated fields: <pf>numEvents</pf> is always incremented; <pf>numRuns</pf> is incremented if <p>predicate</p> is <c>true</c> and <pf>currentRun</pf> is zero; <pf>currentRun</pf> is incremented if <p>predicate</p> is <c>true</c> and set to zero if <p>predicate</p> is <c>false</c>; <pf>longestRun</pf> is set to <pf>currentRun</pf> if <p>predicate</p> is <c>true</c> and <pf>currentRun</pf> is longer than <pf>longestRun</pf>.  If the input <p>history</p> has fields other than <pf>numEvents</pf>, <pf>numRuns</pf>, <pf>currentRun</pf>, or <pf>longestRun</pf>, they are copied unaltered to the output.</ret>
        <error code={s"${errcodeBase + 0}"}>If any of <pf>numEvents</pf>, <pf>numRuns</pf>, <pf>currentRun</pf>, and <pf>longestRun</pf> are less than 0, a "counter out of range" error is raised.</error>
      </doc>
    def errcodeBase = 37000

    def apply(predicate: Boolean, history: PFARecord): PFARecord = {
      var numEvents = history.get("numEvents").asInstanceOf[java.lang.Number].intValue
      var numRuns = history.get("numRuns").asInstanceOf[java.lang.Number].intValue
      var currentRun = history.get("currentRun").asInstanceOf[java.lang.Number].intValue
      var longestRun = history.get("longestRun").asInstanceOf[java.lang.Number].intValue

      if (numEvents < 0  ||  numRuns < 0  ||  currentRun < 0  ||  longestRun < 0)
        throw new PFARuntimeException("counter out of range", errcodeBase + 0, name, pos)

      if (predicate) {
        numEvents += 1

        if (currentRun == 0)
          numRuns += 1

        currentRun += 1

        if (currentRun > longestRun)
          longestRun = currentRun

      }
      else {
        currentRun = 0
      }

      history.multiUpdate(Array("numEvents", "numRuns", "currentRun", "longestRun"), Array(numEvents, numRuns, currentRun, longestRun))
    }
  }
  provide(new UpdateTrigger)

  ////   zValue (ZValue)
  class ZValue(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "zValue"
    def sig = Sigs(List(Sig(List("x" -> P.Double, "meanVariance" -> P.WildRecord("A", ListMap("mean" -> P.Double, "variance" -> P.Double))), P.Double),
                        Sig(List("x" -> P.Double, "meanVariance" -> P.WildRecord("A", ListMap("count" -> P.Double, "mean" -> P.Double, "variance" -> P.Double)), "unbiased" -> P.Boolean), P.Double)))
    def doc =
      <doc>
        <desc>Calculate the z-value between <p>x</p> and a normal distribution with a given mean and variance.</desc>
        <param name="x">Value to test.</param>
        <param name="meanVariance">A record with <pf>mean</pf>, <pf>variance</pf>, and possibly <pf>count</pf>, such as the output of <f>stat.sample.Update</f>.</param>
        <param name="unbiased">If <c>true</c>, use <pf>count</pf> to correct for the bias due to the fact that a variance centered on the mean has one fewer degrees of freedom than the dataset that it was sampled from (Bessel's correction).</param>
        <ret>If <p>unbiased</p> is <c>false</c>, <m>{"(x - mean)/\\sqrt{variance}"}</m>; otherwise <m>{"(x - mean)(1/\\sqrt{variance})\\sqrt{count/(count - 1)}"}</m>.</ret>
      </doc>
    def errcodeBase = 37010

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      if (args.size == 2)
      JavaCode("%s.apply(%s, ((%s)%s).%s, ((%s)%s).%s)",
        javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
        wrapArg(0, args, paramTypes, true),
        javaType(paramTypes(1).asInstanceOf[AvroType], false, true, false),
        wrapArg(1, args, paramTypes, true),
        JVMNameMangle.s("mean"),
        javaType(paramTypes(1).asInstanceOf[AvroType], false, true, false),
        wrapArg(1, args, paramTypes, true),
        JVMNameMangle.s("variance"))
      else
        JavaCode("%s.apply(%s, ((%s)%s).%s, ((%s)%s).%s, ((%s)%s).%s, %s)",
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true),
          javaType(paramTypes(1).asInstanceOf[AvroType], false, true, false),
          wrapArg(1, args, paramTypes, true),
          JVMNameMangle.s("count"),
          javaType(paramTypes(1).asInstanceOf[AvroType], false, true, false),
          wrapArg(1, args, paramTypes, true),
          JVMNameMangle.s("mean"),
          javaType(paramTypes(1).asInstanceOf[AvroType], false, true, false),
          wrapArg(1, args, paramTypes, true),
          JVMNameMangle.s("variance"),
          wrapArg(2, args, paramTypes, true))

    def apply(x: Double, mean: Double, variance: Double): Double = ((x - mean)/Math.sqrt(variance))

    def apply(x: Double, count: Double, mean: Double, variance: Double, unbiased: Boolean): Double =
      if (unbiased)
        ((x - mean)/Math.sqrt(variance)) * Math.sqrt((count) / (count - 1.0))
      else
        ((x - mean)/Math.sqrt(variance))
  }
  provide(new ZValue)

  ////   updateCUSUM (UpdateCUSUM)
  class UpdateCUSUM(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "updateCUSUM"
    def sig = Sig(List("logLikelihoodRatio" -> P.Double, "last" -> P.Double, "reset" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Update a cumulative sum (CUSUM) to detect the transition of a dataset from one distribution to another.</desc>
        <param name="logLikelihoodRatio">The logarithm of the ratio of the likelihood of a value for the alterate and baseline distributions: <m>{"\\ln(\\mbox{alt}_{L}/\\mbox{base}_{L})"}</m>, which is <m>{"\\mbox{alt}_{LL} - \\mbox{base}_{LL}"}</m> where <m>L</m> is likelihood and <m>LL</m> is log-likelihood.  Consider using something like <c>{"""{"-": [{"prob.dist.gaussianLL": [...]}, {"prob.dist.gaussianLL": [...]}]}"""}</c>.</param>
        <param name="last">The previous return value from this function.</param>
        <param name="reset">A low value (usually consistent with the baseline hypothesis, such as 0) at which the cumulative sum resets, rather than accumulate very low values and become insensitive to future changes.</param>
        <ret>An incremented cumulative sum.  The output is <m>{"\\max\\{logLikelihoodRatio + last, reset\\}"}</m>.</ret>
      </doc>
    def errcodeBase = 37020
    def apply(logLikelihoodRatio: Double, last: Double, reset: Double): Double = {
      val out = logLikelihoodRatio + last
      if (out > reset)
        out
      else
        reset
    }
  }
  provide(new UpdateCUSUM)

}
