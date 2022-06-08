#!/usr/bin/env python2

# Copyright (C) 2021 Data Mining Group
# This file is part of POIE.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from poie.datatype import Type
from poie.datatype import FcnType
from poie.datatype import ExceptionType
from poie.datatype import AvroType
from poie.datatype import AvroCompiled
from poie.datatype import AvroNull
from poie.datatype import AvroBoolean
from poie.datatype import AvroInt
from poie.datatype import AvroLong
from poie.datatype import AvroFloat
from poie.datatype import AvroDouble
from poie.datatype import AvroBytes
from poie.datatype import AvroFixed
from poie.datatype import AvroString
from poie.datatype import AvroEnum
from poie.datatype import AvroArray
from poie.datatype import AvroMap
from poie.datatype import AvroRecord
from poie.datatype import AvroField
from poie.datatype import AvroUnion

class Pattern(object):
    """Trait for a type pattern.

    A type pattern is something that a poie.datatype.AvroType is matched against when determining if a PFA function signature can be applied to a given set of arguments.

    It could be as simple as the type itself (e.g. P.Int() matches AvroInt()) or it could be a complex wildcard.
    """
    pass

class Null(Pattern):
    """Matches poie.datatype.AvroNull."""
    def __repr__(self):
        return "P.Null()"

class Boolean(Pattern):
    """Matches poie.datatype.AvroBoolean."""
    def __repr__(self):
        return "P.Boolean()"

class Int(Pattern):
    """Matches poie.datatype.AvroInt."""
    def __repr__(self):
        return "P.Int()"

class Long(Pattern):
    """Matches poie.datatype.AvroLong."""
    def __repr__(self):
        return "P.Long()"

class Float(Pattern):
    """Matches poie.datatype.AvroFloat."""
    def __repr__(self):
        return "P.Float()"

class Double(Pattern):
    """Matches poie.datatype.AvroDouble."""
    def __repr__(self):
        return "P.Double()"

class Bytes(Pattern):
    """Matches poie.datatype.AvroBytes."""
    def __repr__(self):
        return "P.Bytes()"

class String(Pattern):
    """Matches poie.datatype.AvroString."""
    def __repr__(self):
        return "P.String()"

class Array(Pattern):
    """Matches poie.datatype.AvroArray with a given items pattern."""
    def __init__(self, items):
        """:type items: poie.P
        :param items: pattern for the array items
        """
        self._items = items
    @property
    def items(self):
        return self._items
    def __repr__(self):
        return "P.Array(" + repr(self.items) + ")"

class Map(Pattern):
    """Matches poie.datatype.AvroMap with a given values pattern."""
    def __init__(self, values):
        """:type values: poie.P
        :param values: pattern for the map values
        """
        self._values = values
    @property
    def values(self):
        return self._values
    def __repr__(self):
        return "P.Map(" + repr(self.values) + ")"

class Union(Pattern):
    """Matches poie.datatype.AvroUnion with a given set of sub-patterns."""
    def __init__(self, types):
        """:type types: list of poie.P
        :param types: patterns for the subtypes
        """
        self._types = types
    @property
    def types(self):
        return self._types
    def __repr__(self):
        return "P.Union(" + repr(self.types) + ")"

class Fixed(Pattern):
    """Matches poie.datatype.AvroFixed with a given size and an optional name.
    
    Note: not used for any PFA patterns, and probably shouldn't be, even in the future. poie.P.WildFixed is used instead.
    """
    def __init__(self, size, fullName=None):
        """:type size: positive integer
        :param size: width of the fixed-width byte array
        :type fullName: string or ``None``
        :param fullName: optional name of the fixed pattern (if not provided, fixed types of any name would match)
        """
        self._size = size
        self._fullName = fullName
    @property
    def size(self):
        return self._size
    @property
    def fullName(self):
        return self._fullName
    def __repr__(self):
        return "P.Fixed(" + repr(self.size) + ", " + repr(self.fullName) + ")"

class Enum(Pattern):
    """Matches poie.datatype.AvroEnum with a given symbols and an optional name.
    
    Note: not used for any PFA patterns, and probably shouldn't be, even in the future. poie.P.WildEnum is used instead.
    """
    def __init__(self, symbols, fullName=None):
        """:type symbols: list of strings
        :param symbols: names of the enum symbols
        :type fullName: string or ``None``
        :param fullName: optional name of the enum pattern (if not provided, enum types of any name would match)
        """
        self._symbols = symbols
        self._fullName = fullName
    @property
    def symbols(self):
        return self._symbols
    @property
    def fullName(self):
        return self._fullName
    def __repr__(self):
        return "P.Enum(" + repr(self.symbols) + ", " + repr(self.fullName) + ")"

class Record(Pattern):
    """Matches poie.datatype.AvroRecord with given fields and an optional name.
    
    Note: not used for any PFA patterns, and probably shouldn't be, even in the future. poie.P.WildRecord is used instead.
    """
    def __init__(self, fields, fullName=None):
        """:type fields: dict from field names to poie.P
        :param fields: patterns for the record fields
        :type fullName: string or ``None``
        :param fullName: optional name of the record pattern (if not provided, record types of any name would match)
        """
        self._fields = fields
        self._fullName = fullName
    @property
    def fields(self):
        return self._fields
    @property
    def fullName(self):
        return self._fullName
    def __repr__(self):
        return "P.Record(" + repr(self.fields) + ", " + repr(self.fullName) + ")"

class Fcn(Pattern):
    """Matches poie.datatype.FcnType with a given sequence of parameter patterns and return pattern."""
    def __init__(self, params, ret):
        """:type params: list of poie.P
        :param params: patterns for each of the slots in the function-argument's signature (must have only one signature with no wildcards)
        :type ret: poie.P
        :param ret: pattern for the return type of the function-argument
        """
        self._params = params
        self._ret = ret
    @property
    def params(self):
        return self._params
    @property
    def ret(self):
        return self._ret
    def __repr__(self):
        return "P.Fcn(" + repr(self.params) + ", " + repr(self.ret) + ")"

class Wildcard(Pattern):
    """Matches any poie.datatype.AvroType or one of a restricted set.

    Label letters are shared across a signature (e.g. if two wildcards are both labeled "A", then they both have to resolve to the same type).
    """
    def __init__(self, label, oneOf=None):
        """:type label: string
        :param label: label letter (usually one character long, but in principle an arbitrary string)
        :type oneOf: list of poie.datatype.AvroType or ``None``
        :param oneOf: allowed types or ``None`` for unrestricted
        """
        self._label = label
        self._oneOf = oneOf
    @property
    def label(self):
        return self._label
    @property
    def oneOf(self):
        return self._oneOf
    def __repr__(self):
        return "P.Wildcard(" + repr(self.label) + ", " + repr(self.oneOf) + ")"

class WildRecord(Pattern):
    """Matches a poie.datatype.AvroRecord with *at least* the requested set of fields.

    Label letters are shared across a signature (e.g. if two wildcards are both labeled "A", then they both have to resolve to the same type).
    """
    def __init__(self, label, minimalFields=None):
        """:type label: string
        :param label: label letter (usually one character long, but in principle an arbitrary string)
        :type minimalFields: dict from field name to poie.P
        :param minimalFields: fields that a matching record must have and patterns for their respective types
        """
        self._label = label
        self._minimalFields = minimalFields
    @property
    def label(self):
        return self._label
    @property
    def minimalFields(self):
        return self._minimalFields
    def __repr__(self):
        return "P.WildRecord(" + repr(self.label) + ", " + repr(self.minimalFields) + ")"

class WildEnum(Pattern):
    """Matches a poie.datatype.AvroEnum without any constraint on the symbol names.

    Label letters are shared across a signature (e.g. if two wildcards are both labeled "A", then they both have to resolve to the same type).
    """
    def __init__(self, label):
        """:type label: string
        :param label: label letter (usually one character long, but in principle an arbitrary string)
        """
        self._label = label
    @property
    def label(self):
        return self._label
    def __repr__(self):
        return "P.WildEnum(" + repr(self.label) + ")"

class WildFixed(Pattern):
    """Matches a poie.datatype.AvroFixed without any constraint on the size.

    Label letters are shared across a signature (e.g. if two wildcards are both labeled "A", then they both have to resolve to the same type).
    """
    def __init__(self, label):
        """:type label: string
        :param label: label letter (usually one character long, but in principle an arbitrary string)
        """
        self._label = label
    @property
    def label(self):
        return self._label
    def __repr__(self):
        return "P.WildFixed(" + repr(self.label) + ")"

class EnumFields(Pattern):
    """Matches a poie.datatype.AvroEnum whose symbols match the fields of a given record

    Label letters are shared across a signature (e.g. if two wildcards are both labeled "A", then they both have to resolve to the same type).
    """
    def __init__(self, label, wildRecord):
        """:type label: string
        :param label: label letter (usually one character long, but in principle an arbitrary string)
        :type wildRecord: string
        :param wildRecord: label letter of the record (also a wildcard)
        """
        self._label = label
        self._wildRecord = wildRecord
    @property
    def label(self):
        return self._label
    @property
    def wildRecord(self):
        return self._wildRecord
    def __repr__(self):
        return "P.EnumFields(" + repr(self.label) + ", " + repr(self.wildRecord) + ")"

def toType(pat):
    """Convert a pattern to a type, if possible (wildcards can't be converted to types).

    :type pat: poie.P
    :param pat: pattern to convert
    :rtype: poie.datatype.Type
    :return: corresponding type (poie.datatype.Type rather than poie.datatype.AvroType to allow for poie.datatype.FcnType)
    """

    if isinstance(pat, Null): return AvroNull()
    elif isinstance(pat, Boolean): return AvroBoolean()
    elif isinstance(pat, Int): return AvroInt()
    elif isinstance(pat, Long): return AvroLong()
    elif isinstance(pat, Float): return AvroFloat()
    elif isinstance(pat, Double): return AvroDouble()
    elif isinstance(pat, Bytes): return AvroBytes()
    elif isinstance(pat, String): return AvroString()

    elif isinstance(pat, Array): return AvroArray(toType(pat.items))
    elif isinstance(pat, Map): return AvroMap(toType(pat.values))
    elif isinstance(pat, Union): return AvroUnion([toType(x) for x in pat.types])

    elif isinstance(pat, Fixed) and pat.fullName is not None:
        namebits = pat.fullName.split(".")
        if len(namebits) == 1:
            return AvroFixed(pat.size, namebits[-1], None)
        else:
            return AvroFixed(pat.size, namebits[-1], ".".join(namebits[:-1]))
    elif isinstance(pat, Fixed):
        return AvroFixed(pat.size)

    elif isinstance(pat, Enum) and pat.fullName is not None:
        namebits = pat.fullName.split(".")
        if len(namebits) == 1:
            return AvroEnum(pat.symbols, namebits[-1], None)
        else:
            return AvroEnum(pat.symbols, namebits[-1], ".".join(namebits[:-1]))
    elif isinstance(pat, Enum):
        return AvroEnum(pat.symbols)

    elif isinstance(pat, Record) and pat.fullName is not None:
        namebits = pat.fullName.split(".")
        if len(namebits) == 1:
            return AvroRecord([AvroField(k, toType(v)) for k, v in pat.fields.items()], namebits[-1], None)
        else:
            return AvroRecord([AvroField(k, toType(v)) for k, v in pat.fields.items()], namebits[-1], ".".join(namebits[:-1]))
    elif isinstance(pat, Record):
        return AvroRecord([AvroField(k, toType(v)) for k, v in pat.fields.items()])

    elif isinstance(pat, Fcn): return FcnType([toType(x) for x in pat.params()], toType(pat.ret()))

    else: raise Exception

def fromType(t, memo=None):
    """Convert a type to a pattern.

    :type t: poie.datatype.AvroType
    :param t: type to convert
    :rtype: poie.P
    :return: corresponding pattern
    """

    if memo is None:
        memo = set()

    if isinstance(t, AvroNull): return Null()
    elif isinstance(t, AvroBoolean): return Boolean()
    elif isinstance(t, AvroInt): return Int()
    elif isinstance(t, AvroLong): return Long()
    elif isinstance(t, AvroFloat): return Float()
    elif isinstance(t, AvroDouble): return Double()
    elif isinstance(t, AvroBytes): return Bytes()
    elif isinstance(t, AvroString): return String()

    elif isinstance(t, AvroArray): return Array(fromType(t.items, memo))
    elif isinstance(t, AvroMap): return Map(fromType(t.values, memo))
    elif isinstance(t, AvroUnion): return Union([fromType(x, memo) for x in t.types])

    elif isinstance(t, AvroFixed) and t.namespace is not None and t.namespace != "": return Fixed(t.size, t.namespace + "." + t.name)
    elif isinstance(t, AvroFixed): return Fixed(t.size, t.name)
    elif isinstance(t, AvroEnum) and t.namespace is not None and t.namespace != "": return Enum(t.symbols, t.namespace + "." + t.name)
    elif isinstance(t, AvroEnum): return Enum(t.symbols, t.name)
    elif isinstance(t, AvroRecord) and t.namespace is not None and t.namespace != "":
        name = t.namespace + "." + t.name
        if name in memo:
            return Record({}, name)
        else:
            return Record(dict((f.name, fromType(f.avroType, memo.union(set([name])))) for f in t.fields), name)
    elif isinstance(t, AvroRecord):
        if t.name in memo:
            return Record({}, t.name)
        else:
            return Record(dict((f.name, fromType(f.avroType, memo.union(set([t.name])))) for f in t.fields), t.name)
    elif isinstance(t, FcnType): return Fcn([fromType(x, memo) for x in t.params()], fromType(t.ret(), memo))
    elif isinstance(t, ExceptionType): raise IncompatibleTypes("exception type cannot be used in argument patterns")

def mustBeAvro(t):
    """Raise a ``TypeError`` if a given poie.datatype.Type is not a poie.datatype.AvroType; otherwise, pass through.

    :type t: poie.datatype.Type
    :param t: type to check
    :rtype: poie.datatype.AvroType
    :return: the input ``t`` or raise an exception
    """
    if not isinstance(t, AvroType):
        raise TypeError(repr(t) + " is not an Avro type")
    else:
        return t
