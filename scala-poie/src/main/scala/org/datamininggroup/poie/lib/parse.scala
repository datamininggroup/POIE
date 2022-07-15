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

import scala.collection.immutable.ListMap

import org.datamininggroup.poie.ast.LibFcn
import org.datamininggroup.poie.errors.PFARuntimeException
import org.datamininggroup.poie.jvmcompiler.JavaCode
import org.datamininggroup.poie.jvmcompiler.javaSchema

import org.datamininggroup.poie.ast.AstContext
import org.datamininggroup.poie.ast.ExpressionContext
import org.datamininggroup.poie.ast.FcnDef
import org.datamininggroup.poie.ast.FcnRef

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

package object parse {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "parse."

  ////////////////////////////////////////////////////////////////////

  ////   int (ParseInt)
  class ParseInt(val pos: Option[String] = None) extends LibFcn with Function2[String, Int, Int] {
    def name = prefix + "int"
    def sig = Sig(List("str" -> P.String, "base" -> P.Int), P.Int)
    def doc =
      <doc>
        <desc>Parse <p>str</p> and return its value as an integer with base <p>base</p>, if possible.</desc>
        <detail>The string is interpreted as though leading and trailing whitespace were removed and is case-insensitive.</detail>
        <detail>Leading or trailing whitespace and any capitalization is allowed.</detail>
        <error code={s"${errcodeBase + 0}"}>Raises "not an integer" if the string does not conform to "<c>[-+]?[0-9a-z]+</c>" or the number it evaluates to is too large to represent as a 32-bit integer or uses characters as large as or larger than <p>base</p> ('0' through '9' encode 0 through 9 and 'a' through 'z' encode 10 through 35).</error>
        <error code={s"${errcodeBase + 1}"}>Raises "base out of range" if <p>base</p> is less than 2 or greater than 36.</error>
      </doc>
    def errcodeBase = 33000
    val pattern = "[-+]?[0-9a-z]+".r
    def apply(str: String, base: Int): Int =
      try {
        if (base < 2  ||  base > 36)
          throw new PFARuntimeException("base out of range", errcodeBase + 1, name, pos)
        val str2 = str.toLowerCase.trim
        if (pattern.unapplySeq(str2) == None)
          throw new PFARuntimeException("not an integer", errcodeBase + 0, name, pos)
        if (str2.startsWith("+"))
          java.lang.Integer.parseInt(str2.tail, base)
        else
          java.lang.Integer.parseInt(str2, base)
      }
      catch {
        case err: java.lang.NumberFormatException => throw new PFARuntimeException("not an integer", errcodeBase + 0, name, pos)
      }
  }
  provide(new ParseInt)

  ////   long (ParseLong)
  class ParseLong(val pos: Option[String] = None) extends LibFcn with Function2[String, Int, Long] {
    def name = prefix + "long"
    def sig = Sig(List("str" -> P.String, "base" -> P.Int), P.Long)
    def doc =
      <doc>
        <desc>Parse <p>str</p> and return its value as a long integer with base <p>base</p>, if possible.</desc>
        <detail>The string is interpreted as though leading and trailing whitespace were removed and is case-insensitive.</detail>
        <detail>Leading or trailing whitespace and any capitalization is allowed.</detail>
        <error code={s"${errcodeBase + 0}"}>Raises "not a long integer" if the string does not conform to "<c>[-+]?[0-9a-z]+</c>" or the number it evaluates to is too large to represent as a 64-bit integer or uses characters as large as or larger than <p>base</p> ('0' through '9' encode 0 through 9 and 'a' through 'z' encode 10 through 35).</error>
        <error code={s"${errcodeBase + 1}"}>Raises "base out of range" if <p>base</p> is less than 2 or greater than 36.</error>
      </doc>
    def errcodeBase = 33010
    val pattern = "[-+]?[0-9a-z]+".r
    def apply(str: String, base: Int): Long =
      try {
        if (base < 2  ||  base > 36)
          throw new PFARuntimeException("base out of range", errcodeBase + 1, name, pos)
        val str2 = str.toLowerCase.trim
        if (pattern.unapplySeq(str2) == None)
          throw new PFARuntimeException("not a long integer", errcodeBase + 0, name, pos)
        if (str2.startsWith("+"))
          java.lang.Long.parseLong(str2.tail, base)
        else
          java.lang.Long.parseLong(str2, base)
      }
      catch {
        case err: java.lang.NumberFormatException => throw new PFARuntimeException("not a long integer", errcodeBase + 0, name, pos)
      }
  }
  provide(new ParseLong)

  ////   float (ParseFloat)
  class ParseFloat(val pos: Option[String] = None) extends LibFcn with Function1[String, Float] {
    def name = prefix + "float"
    def sig = Sig(List("str" -> P.String), P.Float)
    def doc =
      <doc>
        <desc>Parse <p>str</p> and return its value as a single-precision floating point number.</desc>
        <detail>The string is interpreted as though leading and trailing whitespace were removed and is case-insensitive.</detail>
        <detail>If the string is "nan", the resulting value is not-a-number and if the string is "inf", "+inf", or "-inf", the resulting value is positive infinity, positive infinity, or negative infinity, respectively (see IEEE 754).</detail>
        <detail>If the number's magnitude is too large to be represented as a single-precision float, the resulting value is positive or negative infinity (depending on the sign).  If the numbers magnitude is too small to be represented as a single-precision float, the resulting value is zero.</detail>
        <detail>Leading or trailing whitespace and any capitalization is allowed.</detail>
        <error code={s"${errcodeBase + 0}"}>Raises "not a single-precision float" if the string does not conform to "<c>[-+]?(\\.?[0-9]+|[0-9]+\\.[0-9]*)([eE][-+]?[0-9]+)?</c>", "inf", "+inf", "-inf", or "nan".</error>
      </doc>
    def errcodeBase = 33020
    val pattern = "[-+]?(\\.?[0-9]+|[0-9]+\\.[0-9]*)([eE][-+]?[0-9]+)?".r
    def apply(str: String): Float =
      try {
        val str2 = str.toLowerCase.trim

        if (str2 == "nan")
          java.lang.Float.NaN
        else if (str2 == "inf")
          java.lang.Float.POSITIVE_INFINITY
        else if (str2 == "+inf")
          java.lang.Float.POSITIVE_INFINITY
        else if (str2 == "-inf")
          java.lang.Float.NEGATIVE_INFINITY
        else {
          if (pattern.unapplySeq(str2) == None)
            throw new PFARuntimeException("not a single-precision float", errcodeBase + 0, name, pos)
          java.lang.Float.parseFloat(str2)
        }
      }
      catch {
        case err: java.lang.NumberFormatException => throw new PFARuntimeException("not a single-precision float", errcodeBase + 0, name, pos)
      }
  }
  provide(new ParseFloat)

  ////   double (ParseDouble)
  class ParseDouble(val pos: Option[String] = None) extends LibFcn with Function1[String, Double] {
    def name = prefix + "double"
    def sig = Sig(List("str" -> P.String), P.Double)
    def doc =
      <doc>
        <desc>Parse <p>str</p> and return its value as a double-precision floating point number.</desc>
        <detail>The string is interpreted as though leading and trailing whitespace were removed and is case-insensitive.</detail>
        <detail>If the string is "nan", the resulting value is not-a-number and if the string is "inf", "+inf", or "-inf", the resulting value is positive infinity, positive infinity, or negative infinity, respectively (see IEEE 754).</detail>
        <detail>If the number's magnitude is too large to be represented as a double-precision float, the resulting value is positive or negative infinity (depending on the sign).  If the numbers magnitude is too small to be represented as a double-precision float, the resulting value is zero.</detail>
        <detail>Leading or trailing whitespace and any capitalization is allowed.</detail>
        <error code={s"${errcodeBase + 0}"}>Raises "not a double-precision float" if the string does not conform to "<c>[-+]?(\\.?[0-9]+|[0-9]+\\.[0-9]*)([eE][-+]?[0-9]+)?</c>", "inf", "+inf", "-inf", or "nan".</error>
      </doc>
    def errcodeBase = 33030
    val pattern = "[-+]?(\\.?[0-9]+|[0-9]+\\.[0-9]*)([eE][-+]?[0-9]+)?".r
    def apply(str: String): Double =
      try {
        val str2 = str.toLowerCase.trim

        if (str2 == "nan")
          java.lang.Double.NaN
        else if (str2 == "inf")
          java.lang.Double.POSITIVE_INFINITY
        else if (str2 == "+inf")
          java.lang.Double.POSITIVE_INFINITY
        else if (str2 == "-inf")
          java.lang.Double.NEGATIVE_INFINITY
        else {
          if (pattern.unapplySeq(str2) == None)
            throw new PFARuntimeException("not a double-precision float", errcodeBase + 0, name, pos)
          java.lang.Double.parseDouble(str2)
        }
      }
      catch {
        case err: java.lang.NumberFormatException => throw new PFARuntimeException("not a double-precision float", errcodeBase + 0, name, pos)
      }
  }
  provide(new ParseDouble)
}
