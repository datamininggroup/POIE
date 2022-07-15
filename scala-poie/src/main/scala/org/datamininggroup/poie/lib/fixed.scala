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

import org.datamininggroup.poie.data.PFAFixed

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

package object fixed {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "fixed."

  ////   toBytes (ToBytes)
  class ToBytes(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "toBytes"
    def sig = Sig(List("x" -> P.WildFixed("A")), P.Bytes)
    def doc =
      <doc>
        <desc>Convert fixed-length, named bytes into arbitrary-length, anonymous bytes.</desc>
      </doc>
    def errcodeBase = 20000
    def apply(x: PFAFixed): Array[Byte] = x.bytes
  }
  provide(new ToBytes)

  ////   fromBytes (FromBytes)
  class FromBytes(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "fromBytes"
    def sig = Sig(List("original" -> P.WildFixed("A"), "replacement" -> P.Bytes), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Overlay <p>replacement</p> on top of <p>original</p>.</desc>
        <detail>If <p>replacement</p> is shorter than <p>original</p>, the bytes beyond <p>replacement</p>'s length are taken from <p>original</p>.</detail>
        <detail>If <p>replacement</p> is longer than <p>original</p>, the excess bytes are truncated.</detail>
      </doc>
    def errcodeBase = 20010
    def apply(original: PFAFixed, replacement: Array[Byte]): PFAFixed = original.overlay(replacement)
  }
  provide(new FromBytes)

}
