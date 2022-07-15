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

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.JavaConversions._

import org.apache.avro.AvroRuntimeException
import org.apache.avro.Schema

import org.datamininggroup.poie.ast.LibFcn
import org.datamininggroup.poie.errors.PFARuntimeException
import org.datamininggroup.poie.jvmcompiler.JavaCode
import org.datamininggroup.poie.jvmcompiler.javaSchema
import org.datamininggroup.poie.jvmcompiler.javaType
import org.datamininggroup.poie.jvmcompiler.JVMNameMangle
import org.datamininggroup.poie.options.EngineOptions

import org.datamininggroup.poie.ast.AstContext
import org.datamininggroup.poie.ast.ExpressionContext
import org.datamininggroup.poie.ast.FcnDef
import org.datamininggroup.poie.ast.FcnRef

import org.datamininggroup.poie.data.PFAArray
import org.datamininggroup.poie.data.PFARecord
import org.datamininggroup.poie.data.PFAEnumSymbol
import org.datamininggroup.poie.data.ComparisonOperator

import org.datamininggroup.poie.signature.P
import org.datamininggroup.poie.signature.Sig
import org.datamininggroup.poie.signature.Signature
import org.datamininggroup.poie.signature.Sigs

import org.datamininggroup.poie.datatype.AvroConversions.schemaToAvroType
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

package object tree {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "model.tree."

  //////////////////////////////////////////////////////////////////// 

  object SimpleComparison {
    def isCompatibleArray(valueSchema: Schema, fieldSchema: Schema): Boolean =
      valueSchema.getType == Schema.Type.ARRAY  &&  schemaToAvroType(valueSchema.getElementType).accepts(schemaToAvroType(fieldSchema))

    def apply(datum: PFARecord, field: PFAEnumSymbol, operator: String, value: Any, fieldSchema: Schema, valueSchema: Schema, missingOperators: Boolean, code1: Int, code2: Int, fcnName: String, pos: Option[String]): Boolean = {
      if (operator == "alwaysTrue")
        true
      else if (operator == "alwaysFalse")
        false
      else if (missingOperators  &&  operator == "isMissing")
        datum.get(field.value) == null
      else if (missingOperators  &&  operator == "notMissing")
        datum.get(field.value) != null
      else if (operator == "in"  ||  operator == "notIn") {
        val itemType =
          if (isCompatibleArray(valueSchema, fieldSchema))
            valueSchema.getElementType
          else if (valueSchema.getType == Schema.Type.UNION)
            valueSchema.getTypes.find(isCompatibleArray(_, fieldSchema)) match {
              case Some(x) => x.getElementType
              case None => throw new PFARuntimeException("bad value type", code1, fcnName, pos)
            }
          else
            throw new PFARuntimeException("bad value type", code1, fcnName, pos)

        val vector = value.asInstanceOf[PFAArray[Any]].toVector

        val fieldValue = (datum.get(field.value), itemType.getType) match {
          case (x: java.lang.Number, Schema.Type.INT) => x.intValue
          case (x: java.lang.Number, Schema.Type.LONG) => x.longValue
          case (x: java.lang.Number, Schema.Type.FLOAT) => x.floatValue
          case (x: java.lang.Number, Schema.Type.DOUBLE) => x.doubleValue
          case (x, _) => x
        }

        val containedInVector = vector.contains(fieldValue)

        if (operator == "in")
          containedInVector
        else
          !containedInVector
      }

      else {
        fieldSchema.getType match {
          case Schema.Type.INT | Schema.Type.LONG | Schema.Type.FLOAT | Schema.Type.DOUBLE => {
            val fieldNumber = datum.get(field.value).asInstanceOf[java.lang.Number].doubleValue
            val valueNumber = value match {
              case null => throw new PFARuntimeException("bad value type", code1, fcnName, pos)
              case x: Int => x.toDouble
              case x: Long => x.toDouble
              case x: Float => x.toDouble
              case x: Double => x
              case x: java.lang.Number => x.doubleValue
              case _ => throw new PFARuntimeException("bad value type", code1, fcnName, pos)
            }

            operator match {
              case "<=" => fieldNumber <= valueNumber
              case "<" => fieldNumber < valueNumber
              case ">=" => fieldNumber >= valueNumber
              case ">" => fieldNumber > valueNumber
              case "==" => fieldNumber == valueNumber
              case "!=" => fieldNumber != valueNumber
              case _ => throw new PFARuntimeException("invalid comparison operator", code2, fcnName, pos)
            }
          }

          case Schema.Type.STRING => {
            val fieldString = datum.get(field.value).asInstanceOf[String]
            val valueString = value match {
              case null => throw new PFARuntimeException("bad value type", code1, fcnName, pos)
              case x: String => x
              case _ => throw new PFARuntimeException("bad value type", code1, fcnName, pos)
            }

            operator match {
              case "==" => fieldString == valueString
              case "!=" => fieldString != valueString
              case "<=" => (new ComparisonOperator(valueSchema, -3)).apply(fieldString, valueString)
              case "<" => (new ComparisonOperator(valueSchema, -2)).apply(fieldString, valueString)
              case ">=" => (new ComparisonOperator(valueSchema, 2)).apply(fieldString, valueString)
              case ">" => (new ComparisonOperator(valueSchema, 3)).apply(fieldString, valueString)
              case _ => throw new PFARuntimeException("invalid comparison operator", code2, fcnName, pos)
            }
          }

          case _ => {
            if (!schemaToAvroType(valueSchema).accepts(schemaToAvroType(fieldSchema)))
              throw new PFARuntimeException("bad value type", code1, fcnName, pos)

            val fieldObject = (datum.get(field.value), valueSchema.getType) match {
              case (x: java.lang.Integer, Schema.Type.INT) => x
              case (x: java.lang.Integer, Schema.Type.LONG) => java.lang.Long.valueOf(x.longValue)
              case (x: java.lang.Integer, Schema.Type.FLOAT) => java.lang.Float.valueOf(x.floatValue)
              case (x: java.lang.Integer, Schema.Type.DOUBLE) => java.lang.Double.valueOf(x.doubleValue)
              case (x: java.lang.Long, Schema.Type.LONG) => x
              case (x: java.lang.Long, Schema.Type.FLOAT) => java.lang.Float.valueOf(x.floatValue)
              case (x: java.lang.Long, Schema.Type.DOUBLE) => java.lang.Double.valueOf(x.doubleValue)
              case (x: java.lang.Float, Schema.Type.FLOAT) => x
              case (x: java.lang.Float, Schema.Type.DOUBLE) => java.lang.Double.valueOf(x.doubleValue)
              case (x: java.lang.Double, Schema.Type.DOUBLE) => x
              case (x, _) => x
            }

            operator match {
              case "<=" => (new ComparisonOperator(valueSchema, -3)).apply(fieldObject, value.asInstanceOf[AnyRef])
              case "<" => (new ComparisonOperator(valueSchema, -2)).apply(fieldObject, value.asInstanceOf[AnyRef])
              case "!=" => (new ComparisonOperator(valueSchema, -1)).apply(fieldObject, value.asInstanceOf[AnyRef])
              case "==" => (new ComparisonOperator(valueSchema, 1)).apply(fieldObject, value.asInstanceOf[AnyRef])
              case ">=" => (new ComparisonOperator(valueSchema, 2)).apply(fieldObject, value.asInstanceOf[AnyRef])
              case ">" => (new ComparisonOperator(valueSchema, 3)).apply(fieldObject, value.asInstanceOf[AnyRef])
              case _ => throw new PFARuntimeException("invalid comparison operator", code2, fcnName, pos)
            }
          }
        }
      }
    }

    def removeNull(schema: Schema): Schema = schema.getType match {
      case Schema.Type.UNION => {
        val withoutNull = schema.getTypes filter {_.getType != Schema.Type.NULL}
        if (withoutNull.isEmpty)
          schema
        else if (withoutNull.size == 1)
          withoutNull.head
        else
          Schema.createUnion(withoutNull)
      }
      case _ => schema
    }
  }

  ////   simpleTest (SimpleTest)
  class SimpleTest(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "simpleTest"
    def sig = Sig(List("datum" -> P.WildRecord("D", Map()), "comparison" -> P.WildRecord("T", ListMap("field" -> P.EnumFields("F", "D"), "operator" -> P.String, "value" -> P.Wildcard("V")))), P.Boolean)
    def doc =
      <doc>
        <desc>Determine if <p>datum</p> passes a test defined by <p>comparison</p>.</desc>
        <param name="datum">Sample value to test.</param>
        <param name="comparison">Record that describes a test.
          <paramField name="field">Field name from <p>datum</p>: the enumeration type must include all fields of <tp>D</tp> in their declaration order.</paramField>
          <paramField name="operator">One of the following: "==" (equal), "!=" (not equal), "&lt;" (less than), "&lt;=" (less or equal), "&gt;" (greater than), "&gt;=" (greater or equal), "in" (member of a set), "notIn" (not a member of a set), "alwaysTrue" (ignore <pf>value</pf>, return <c>true</c>), "alwaysFalse" (ignore <pf>value</pf>, return <c>false</c>), "isMissing" (ignore <pf>value</pf>, return <c>true</c> iff the field of <p>datum</p> is <c>null</c>), and "notMissing" (ignore <pf>value</pf>, return <c>false</c> iff the field of <p>datum</p> is <c>null</c>).</paramField>
          <paramField name="value">Value to which the field of <p>datum</p> is compared.</paramField>
        </param>
        <ret>Returns <c>true</c> if the field of <p>datum</p> &lt;op&gt; <pf>value</pf> is <c>true</c>, <c>false</c> otherwise, where &lt;op&gt; is the <pf>operator</pf>.</ret>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid comparison operator" if <pf>operator</pf> is not one of "==", "!=", "&lt;", "&lt;=", "&gt;", "&gt;=", "in", "notIn", "alwaysTrue", "alwaysFalse", "isMissing", "notMissing".</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "bad value type" if the <pf>field</pf> of <p>datum</p> and <tp>V</tp> are not both numbers and the <pf>field</pf> cannot be upcast to <tp>V</tp>.</error>
      </doc>
    def errcodeBase = 32000

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("""(new Object() {
public boolean apply(org.datamininggroup.poie.data.PFARecord datum, %s comparison) {
org.datamininggroup.poie.data.PFAEnumSymbol field = (org.datamininggroup.poie.data.PFAEnumSymbol)(comparison.%s);
return org.datamininggroup.poie.lib.model.tree.package$SimpleComparison$.MODULE$.apply(datum, field, comparison.%s, comparison.%s, datum.fieldTypes()[field.value()], comparison.fieldTypes()[comparison.fieldIndex("value")], true, %s, %s, "%s", %s);
} }).apply((org.datamininggroup.poie.data.PFARecord)%s, (%s)%s)""",
        javaType(paramTypes(1).asInstanceOf[AvroRecord], true, true, false),
        JVMNameMangle.s("field"),
        JVMNameMangle.s("operator"),
        JVMNameMangle.s("value"),
        (errcodeBase + 1).toString,
        (errcodeBase + 0).toString,
        name,
        posToJava,
        wrapArg(0, args, paramTypes, true),
        javaType(paramTypes(1).asInstanceOf[AvroRecord], true, true, false),
        wrapArg(1, args, paramTypes, true))

  }
  provide(new SimpleTest)

  ////   missingTest (MissingTest)
  class MissingTest(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "missingTest"
    def sig = Sig(List("datum" -> P.WildRecord("D", Map()), "comparison" -> P.WildRecord("T", ListMap("field" -> P.EnumFields("F", "D"), "operator" -> P.String, "value" -> P.Wildcard("V")))), P.Union(List(P.Null, P.Boolean)))
    def doc =
      <doc>
        <desc>Determine if <p>datum</p> passes a test defined by <p>comparison</p>, allowing for missing values.</desc>
        <param name="datum">Sample value to test.</param>
        <param name="comparison">Record that describes a test.
          <paramField name="field">Field name from <p>datum</p>: the enumeration type must include all fields of <tp>D</tp> in their declaration order.</paramField>
          <paramField name="operator">One of the following: "==" (equal), "!=" (not equal), "&lt;" (less than), "&lt;=" (less or equal), "&gt;" (greater than), "&gt;=" (greater or equal), "in" (member of a set), "notIn" (not a member of a set), "alwaysTrue" (ignore <pf>value</pf>, return <c>true</c>), "alwaysFalse" (ignore <pf>value</pf>, return <c>false</c>).</paramField>
          <paramField name="value">Value to which the field of <p>datum</p> is compared.</paramField>
        </param>
        <ret>If the field of <p>datum</p> is <c>null</c>, this function returns <c>null</c> (unknown test result).  Otherwise, it returns <p>datum</p> field &lt;op&gt; <pf>value</pf>, where &lt;op&gt; is the <pf>operator</pf></ret>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid comparison operator" if <pf>operator</pf> is not one of "==", "!=", "&lt;", "&lt;=", "&gt;", "&gt;=", "in", "notIn", "alwaysTrue", "alwaysFalse".</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "bad value type" if the <pf>field</pf> of <p>datum</p> and <tp>V</tp> are not both numbers and the <pf>field</pf> cannot be upcast to <tp>V</tp>.</error>
      </doc>
    def errcodeBase = 32010

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("""(new Object() {
public java.lang.Boolean apply(org.datamininggroup.poie.data.PFARecord datum, %s comparison) {
org.datamininggroup.poie.data.PFAEnumSymbol field = (org.datamininggroup.poie.data.PFAEnumSymbol)(comparison.%s);
if (datum.get(field.value()) == null)
    return null;
return java.lang.Boolean.valueOf(org.datamininggroup.poie.lib.model.tree.package$SimpleComparison$.MODULE$.apply(datum, field, comparison.%s, comparison.%s, org.datamininggroup.poie.lib.model.tree.package$SimpleComparison$.MODULE$.removeNull(datum.fieldTypes()[field.value()]), comparison.fieldTypes()[comparison.fieldIndex("value")], false, %s, %s, "%s", %s));
} }).apply((org.datamininggroup.poie.data.PFARecord)%s, (%s)%s)""",
        javaType(paramTypes(1).asInstanceOf[AvroRecord], true, true, false),
        JVMNameMangle.s("field"),
        JVMNameMangle.s("operator"),
        JVMNameMangle.s("value"),
        (errcodeBase + 1).toString,
        (errcodeBase + 0).toString,
        name,
        posToJava,
        wrapArg(0, args, paramTypes, true),
        javaType(paramTypes(1).asInstanceOf[AvroRecord], true, true, false),
        wrapArg(1, args, paramTypes, true))
  }
  provide(new MissingTest)

  ////   compoundTest (CompoundTest)
  class CompoundTest(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "compoundTest"
    def sig = Sig(List("datum" -> P.WildRecord("D", Map()), "operator" -> P.String, "comparisons" -> P.Array(P.WildRecord("T", Map())), "test" -> P.Fcn(List(P.Wildcard("D"), P.Wildcard("T")), P.Boolean)), P.Boolean)
    def doc =
      <doc>
        <desc>Apply <p>test</p> to an array of <p>comparisons</p>, returning their logical and, or, or xor, depending on <p>operator</p>.</desc>
        <param name="datum">Simple value to test.</param>
        <param name="operator">If "and", return <c>true</c> if no <c>false</c> is encountered, if "or", return <c>true</c> if any <c>true</c> is encountered, and if "xor", return <c>true</c> if an odd number of <c>true</c> is encountered among the <p>comparisons</p>.</param>
        <param name="comparisons">Array of records that describe the tests.</param>
        <param name="test">Test function applied to each item of <p>comparisons</p> until the result is certain.</param>
        <ret>Logical combination of <p>comparisons</p>.</ret>
        <detail>If <p>operator</p> is "and", the <p>test</p> will only be applied until the first <c>false</c> is encountered. If <p>operator</p> is "or", the <p>test</p> will only be applied until the first <c>true</c> is encountered. If <p>operator</p> is "xor", the <p>test</p> will be applied to all items of <p>comparisons</p>.</detail>
        <error code={s"${errcodeBase + 0}"}>If <p>operator</p> is not "and", "or", or "xor", an "unrecognized logical operator" error is raised.</error>
      </doc>
    def errcodeBase = 32020
    def apply(datum: PFARecord, operator: String, comparisons: PFAArray[PFARecord], test: (PFARecord, PFARecord) => Boolean): Boolean = operator match {
      case "and" => comparisons.toVector.forall(comparison => test(datum, comparison))
      case "or" => comparisons.toVector.exists(comparison => test(datum, comparison))
      case "xor" => comparisons.toVector.map(comparison => if (test(datum, comparison)) 1 else 0).sum % 2 == 1
      case _ => throw new PFARuntimeException("unrecognized logical operator", errcodeBase + 0, name, pos)
    }

  }
  provide(new CompoundTest)

  ////   surrogateTest (SurrogateTest)
  class SurrogateTest(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "surrogateTest"
    def sig = Sig(List("datum" -> P.WildRecord("D", Map()), "comparisons" -> P.Array(P.WildRecord("T", Map())), "missingTest" -> P.Fcn(List(P.Wildcard("D"), P.Wildcard("T")), P.Union(List(P.Null, P.Boolean)))), P.Boolean)
    def doc =
      <doc>
        <desc>Apply <p>missingTest</p> to an array of <p>comparisons</p> until one yields a non-null result.</desc>
        <param name="datum">Sample value to test.</param>
        <param name="comparisons">Array of records that describe the tests.</param>
        <param name="missingTest">Test function applied to each item of <p>comparisons</p> until one returns a non-null result.</param>
        <ret>Returns the value of the first test that returns <c>true</c> or <c>false</c>.</ret>
        <error code={s"${errcodeBase + 0}"}>If all tests return <c>null</c>, this function raises a "no successful surrogate" error.</error>
      </doc>
    def errcodeBase = 32030
    def apply(datum: PFARecord, comparisons: PFAArray[PFARecord], missingTest: (PFARecord, PFARecord) => java.lang.Boolean): Boolean = {
      var result = false
      if (comparisons.toVector.indexWhere(comparison =>
        missingTest(datum, comparison) match {
          case null => false
          case x: java.lang.Boolean => {
            result = x.booleanValue
            true
          }
        }) == -1)
        throw new PFARuntimeException("no successful surrogate", errcodeBase + 0, name, pos)
      else
        result
    }
  }
  provide(new SurrogateTest)

  ////   simpleWalk (SimpleWalk)
  class SimpleWalk(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "simpleWalk"
    def sig = Sig(List(
      "datum" -> P.WildRecord("D", Map()),
      "treeNode" -> P.WildRecord("T", ListMap("pass" -> P.Union(List(P.WildRecord("T", Map()), P.Wildcard("S"))), "fail" -> P.Union(List(P.WildRecord("T", Map()), P.Wildcard("S"))))),
      "test" -> P.Fcn(List(P.Wildcard("D"), P.Wildcard("T")), P.Boolean)
    ), P.Wildcard("S"))
    def doc =
      <doc>
        <desc>Descend through a tree, testing the fields of <p>datum</p> with the <p>test</p> function using <p>treeNode</p> to define the comparison, continuing to <pf>pass</pf> or <pf>fail</pf> until reaching a leaf node of type <tp>S</tp> (score).</desc>
        <param name="datum">Sample value to test.</param>
        <param name="treeNode">Node of the tree, which contains a predicate to be interpreted by <p>test</p>.
          <paramField name="pass">Branch to follow if <p>test</p> returns <c>true</c>.</paramField>
          <paramField name="fail">Branch to follow if <p>test</p> returns <c>false</c>.</paramField>
        </param>
        <param name="test">Test function that converts <p>datum</p> and <p>treeNode</p> into <c>true</c> or <c>false</c>.</param>
        <ret>Leaf node of type <tp>S</tp>, which must be different from the tree nodes.  For a classification tree, <tp>S</tp> could be a string or an enumeration set.  For a regression tree, <tp>S</tp> would be a numerical type.  For a multivariate regression tree, <tp>S</tp> would be an array of numbers, etc.</ret>
      </doc>
    def errcodeBase = 32040
    @tailrec
    final def apply(datum: PFARecord, treeNode: PFARecord, test: (PFARecord, PFARecord) => Boolean): AnyRef = {
      val next =
        if (test(datum, treeNode))
          treeNode.get("pass")
        else
          treeNode.get("fail")
      next match {
        case x: PFARecord if (treeNode.getSchema.getFullName == x.getSchema.getFullName) => apply(datum, x, test)
        case x => x
      }
    }
  }
  provide(new SimpleWalk)

  ////   missingWalk (MissinWalk)
  class MissingWalk(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "missingWalk"
    def sig = Sig(List(
      "datum" -> P.WildRecord("D", Map()),
      "treeNode" -> P.WildRecord("T", ListMap("pass" -> P.Union(List(P.WildRecord("T", Map()), P.Wildcard("S"))),
                                              "fail" -> P.Union(List(P.WildRecord("T", Map()), P.Wildcard("S"))),
                                              "missing" -> P.Union(List(P.WildRecord("T", Map()), P.Wildcard("S"))))),
      "test" -> P.Fcn(List(P.Wildcard("D"), P.Wildcard("T")), P.Union(List(P.Null, P.Boolean)))
    ), P.Wildcard("S"))
    def doc =
      <doc>
        <desc>Descend through a tree, testing the fields of <p>datum</p> with the <p>test</p> function using <p>treeNode</p> to define the comparison, continuing to <pf>pass</pf>, <pf>fail</pf>, or <pf>missing</pf> until reaching a leaf node of type <tp>S</tp> (score).</desc>
        <param name="datum">Sample value to test.</param>
        <param name="treeNode">Node of the tree, which contains a predicate to be interpreted by <p>test</p>.
          <paramField name="pass">Branch to follow if <p>test</p> returns <c>true</c>.</paramField>
          <paramField name="fail">Branch to follow if <p>test</p> returns <c>false</c>.</paramField>
          <paramField name="missing">Branch to follow if <p>test</p> returns <c>null</c>.</paramField>
        </param>
        <param name="test">Test function that converts <p>datum</p> and <p>treeNode</p> into <c>true</c>, <c>false</c>, or <c>null</c>.</param>
        <ret>Leaf node of type <tp>S</tp>, which must be different from the tree nodes.  For a classification tree, <tp>S</tp> could be a string or an enumeration set.  For a regression tree, <tp>S</tp> would be a numerical type.  For a multivariate regression tree, <tp>S</tp> would be an array of numbers, etc.</ret>
      </doc>
    def errcodeBase = 32050
    @tailrec
    final def apply(datum: PFARecord, treeNode: PFARecord, test: (PFARecord, PFARecord) => java.lang.Boolean): AnyRef = {
      val next = test(datum, treeNode) match {
        case null => treeNode.get("missing")
        case java.lang.Boolean.TRUE => treeNode.get("pass")
        case java.lang.Boolean.FALSE => treeNode.get("fail")
      }
      next match {
        case x: PFARecord if (treeNode.getSchema.getFullName == x.getSchema.getFullName) => apply(datum, x, test)
        case x => x
      }
    }
  }
  provide(new MissingWalk)

  ////   simpleTree (SimpleTree)
  object SimpleTree {
    class Walker(test: (PFARecord, PFARecord) => Boolean) {
      @tailrec
      final def apply(datum: PFARecord, treeNode: PFARecord): AnyRef = {
        val next =
          if (test(datum, treeNode))
            treeNode.get("pass")
          else
            treeNode.get("fail")
        next match {
          case x: PFARecord if (treeNode.getSchema.getFullName == x.getSchema.getFullName) => apply(datum, x)
          case x => x
        }
      }
    }
  }
  class SimpleTree(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "simpleTree"
    def sig = Sig(List("datum" -> P.WildRecord("D", Map()), "treeNode" -> P.WildRecord("T", ListMap("field" -> P.EnumFields("F", "D"), "operator" -> P.String, "value" -> P.Wildcard("V"), "pass" -> P.Union(List(P.WildRecord("T", Map()), P.Wildcard("S"))), "fail" -> P.Union(List(P.WildRecord("T", Map()), P.Wildcard("S")))))), P.Wildcard("S"))
    def doc =
      <doc>
        <desc>Descend through a tree, testing <p>datum</p> with <pf>field</pf>, <pf>operator</pf>, <pf>value</pf>, following <pf>pass</pf> or <pf>fail</pf> until reaching a leaf node of type <tp>S</tp> (score).</desc>
        <param name="datum">Sample value to test.</param>
        <param name="treeNode">Record that describes a tree node (predicate test with branches).
          <paramField name="field">Field name from <p>datum</p>: the enumeration type must include all fields of <tp>D</tp> in their declaration order.</paramField>
          <paramField name="operator">One of the following: "==" (equal), "!=" (not equal), "&lt;" (less than), "&lt;=" (less or equal), "&gt;" (greater than), "&gt;=" (greater or equal), "in" (member of a set), "notIn" (not a member of a set), "alwaysTrue" (ignore <pf>value</pf>, return <c>true</c>), "alwaysFalse" (ignore <pf>value</pf>, return <c>false</c>), "isMissing" (ignore <pf>value</pf>, return <c>true</c> iff the field of <p>datum</p> is <c>null</c>), and "notMissing" (ignore <pf>value</pf>, return <c>false</c> iff the field of <p>datum</p> is <c>null</c>).</paramField>
          <paramField name="value">Value to which the field of <p>datum</p> is compared.</paramField>
          <paramField name="pass">Branch to follow if the comparison is successful.</paramField>
          <paramField name="fail">Branch to follow if the comparison fails.</paramField>
        </param>
        <ret>Leaf node of type <tp>S</tp>, which must be different from the tree nodes.  For a classification tree, <tp>S</tp> could be a string or an enumeration set.  For a regression tree, <tp>S</tp> would be a numerical type.  For a multivariate regression tree, <tp>S</tp> would be an array of numbers, etc.</ret>
        <detail>This is a convenience function, a combination of <f>model.tree.simpleWalk</f> with <f>model.tree.simpleTest</f>.</detail>
        <error code={s"${errcodeBase + 0}"}>Raises an "invalid comparison operator" if <pf>operator</pf> is not one of "==", "!=", "&lt;", "&lt;=", "&gt;", "&gt;=", "in", "notIn", "alwaysTrue", "alwaysFalse", "isMissing", "notMissing".</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "bad value type" if the <pf>field</pf> of <p>datum</p> and <tp>V</tp> are not both numbers and the <pf>field</pf> cannot be upcast to <tp>V</tp>.</error>
      </doc>
    def errcodeBase = 32060

    override def javaRef(fcnType: FcnType): JavaCode =
        JavaCode("""(new %s$Walker(new scala.runtime.AbstractFunction2<org.datamininggroup.poie.data.PFARecord, %s, java.lang.Boolean>() {
public java.lang.Boolean apply(Object datum, Object comparison) {
return apply((org.datamininggroup.poie.data.PFARecord)datum, (%s)comparison);
}
public java.lang.Boolean apply(org.datamininggroup.poie.data.PFARecord datum, %s comparison) {
org.datamininggroup.poie.data.PFAEnumSymbol field = (org.datamininggroup.poie.data.PFAEnumSymbol)(comparison.%s);
return java.lang.Boolean.valueOf(org.datamininggroup.poie.lib.model.tree.package$SimpleComparison$.MODULE$.apply(datum, field, comparison.%s, comparison.%s, datum.fieldTypes()[field.value()], comparison.fieldTypes()[comparison.fieldIndex("value")], true, %s, %s, "%s", %s));
} }))""".format(this.getClass.getName,
                javaType(fcnType.params(1).asInstanceOf[AvroRecord], true, true, false),
                javaType(fcnType.params(1).asInstanceOf[AvroRecord], true, true, false),
                javaType(fcnType.params(1).asInstanceOf[AvroRecord], true, true, false),
                JVMNameMangle.s("field"),
                JVMNameMangle.s("operator"),
                JVMNameMangle.s("value"),
                (errcodeBase + 1).toString,
                (errcodeBase + 0).toString,
                name,
                posToJava))
  }
  provide(new SimpleTree)

}
