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

import org.datamininggroup.poie.data.PFAEnumSymbol

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

import org.datamininggroup.poie.signature.Sig
import org.datamininggroup.poie.signature.P

package object enum {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "enum."

  ////   toString (ToString)
  class ToString(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "toString"
    def sig = Sig(List("x" -> P.WildEnum("A")), P.String)
    def doc =
      <doc>
        <desc>Return the string representation of an enum.</desc>
      </doc>
    def errcodeBase = 19000
    def apply(x: PFAEnumSymbol): String = x.intToStr(x.value)
  }
  provide(new ToString)

  ////   toInt (ToInt)
  class ToInt(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "toInt"
    def sig = Sig(List("x" -> P.WildEnum("A")), P.Int)
    def doc =
      <doc>
        <desc>Return the integer representation of an enum.</desc>
      </doc>
    def errcodeBase = 19010
    def apply(x: PFAEnumSymbol): Int = x.value
  }
  provide(new ToInt)

  ////   numSymbols (NumSymbols)
  class NumSymbols(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "numSymbols"
    def sig = Sig(List("x" -> P.WildEnum("A")), P.Int)
    def doc =
      <doc>
        <desc>Return the number of symbols associated with this enum (a constant).</desc>
      </doc>
    def errcodeBase = 19020
    def apply(x: PFAEnumSymbol): Int = x.numSymbols
  }
  provide(new NumSymbols)

}
