
# Copyright (C) 2021 Data Mining Group
# 
# This file is part of POIE
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

import math

from poie.fcn import Fcn
from poie.fcn import LibFcn
from poie.signature import Sig
from poie.signature import Sigs
from poie.datatype import *
from poie.errors import *
from poie.lib.core import checkForOverflow
import poie.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "m."

#################################################################### constants (0-arity side-effect free functions)

class Pi(LibFcn):
    name = prefix + "pi"
    sig = Sig([], P.Double())
    errcodeBase = 27000
    def genpy(self, paramTypes, args, pos):
        return "math.pi"
    def __call__(self, state, scope, pos, paramTypes):
        return math.pi
provide(Pi())

class E(LibFcn):
    name = prefix + "e"
    sig = Sig([], P.Double())
    errcodeBase = 27010
    def genpy(self, paramTypes, args, pos):
        return "math.e"
    def __call__(self, state, scope, pos, paramTypes):
        return math.e
provide(E())

#################################################################### basic functions (alphabetical order)

anyNumber = set([AvroInt(), AvroLong(), AvroFloat(), AvroDouble()])

class Abs(LibFcn):
    name = prefix + "abs"
    sig = Sig([{"x": P.Wildcard("A", anyNumber)}], P.Wildcard("A"))
    errcodeBase = 27020
    def __call__(self, state, scope, pos, paramTypes, x):
        return checkForOverflow(paramTypes[0], abs(x), self.errcodeBase + 0, self.errcodeBase + 1, self.name, pos)
provide(Abs())

class ACos(LibFcn):
    name = prefix + "acos"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27030
    def __call__(self, state, scope, pos, paramTypes, x):
        try:
            return math.acos(x)
        except ValueError:
            return float("nan")
provide(ACos())

class ASin(LibFcn):
    name = prefix + "asin"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27040
    def __call__(self, state, scope, pos, paramTypes, x):
        try:
            return math.asin(x)
        except ValueError:
            return float("nan")
provide(ASin())

class ATan(LibFcn):
    name = prefix + "atan"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27050
    def genpy(self, paramTypes, args, pos):
        return "math.atan({0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return math.atan(x)
provide(ATan())

class ATan2(LibFcn):
    name = prefix + "atan2"
    sig = Sig([{"y": P.Double()}, {"x": P.Double()}], P.Double())
    errcodeBase = 27060
    def genpy(self, paramTypes, args, pos):
        return "math.atan2({0}, {1})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x, y):
        return math.atan2(x, y)
provide(ATan2())

class Ceil(LibFcn):
    name = prefix + "ceil"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27070
    def genpy(self, paramTypes, args, pos):
        return "math.ceil({0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return math.ceil(x)
provide(Ceil())

class CopySign(LibFcn):
    name = prefix + "copysign"
    sig = Sig([{"mag": P.Wildcard("A", anyNumber)}, {"sign": P.Wildcard("A")}], P.Wildcard("A"))
    errcodeBase = 27080
    def __call__(self, state, scope, pos, paramTypes, mag, sign):
        return abs(mag) * (-1 if sign < 0 else 1)
provide(CopySign())

class Cos(LibFcn):
    name = prefix + "cos"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27090
    def __call__(self, state, scope, pos, paramTypes, x):
        if math.isinf(x) or math.isnan(x):
            return float("nan")
        else:
            return math.cos(x)
provide(Cos())

class CosH(LibFcn):
    name = prefix + "cosh"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27100
    def genpy(self, paramTypes, args, pos):
        return "math.cosh({0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return math.cosh(x)
provide(CosH())

class Exp(LibFcn):
    name = prefix + "exp"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27110
    def genpy(self, paramTypes, args, pos):
        return "math.exp({0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return math.exp(x)
provide(Exp())

class ExpM1(LibFcn):
    name = prefix + "expm1"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27120
    def genpy(self, paramTypes, args, pos):
        return "(math.exp({0}) - 1.0)".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return math.exp(x) - 1.0
provide(ExpM1())

class Floor(LibFcn):
    name = prefix + "floor"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27130
    def genpy(self, paramTypes, args, pos):
        return "math.floor({0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return math.floor(x)
provide(Floor())

class Hypot(LibFcn):
    name = prefix + "hypot"
    sig = Sig([{"x": P.Double()}, {"y": P.Double()}], P.Double())
    errcodeBase = 27140
    def __call__(self, state, scope, pos, paramTypes, x, y):
        if math.isinf(x) or math.isinf(y):
            return float("inf")
        else:
            return math.sqrt(x**2 + y**2)
provide(Hypot())

class Ln(LibFcn):
    name = prefix + "ln"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27150
    def __call__(self, state, scope, pos, paramTypes, x):
        if x < 0.0:
            return float("nan")
        elif x == 0.0:
            return float("-inf")
        else:
            return math.log(x)
provide(Ln())

class Log10(LibFcn):
    name = prefix + "log10"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27160
    def __call__(self, state, scope, pos, paramTypes, x):
        if x < 0.0:
            return float("nan")
        elif x == 0.0:
            return float("-inf")
        else:
            return math.log10(x)
provide(Log10())

class Log(LibFcn):
    name = prefix + "log"
    sig = Sig([{"x": P.Double()}, {"base": P.Int()}], P.Double())
    errcodeBase = 27170
    def __call__(self, state, scope, pos, paramTypes, x, base):
        if base <= 0:
            raise PFARuntimeException("base must be positive", self.errcodeBase + 0, self.name, pos)
        elif math.isnan(x):
            return float("nan")
        elif x < 0.0:
            return float("nan")
        elif x == 0.0:
            return float("-inf")
        elif base == 1:
            if x < 1.0:
                return float("-inf")
            elif x == 1.0:
                return float("nan")
            elif x > 1.0:
                return float("inf")
        else:
            return math.log(x, base)
provide(Log())

class Ln1p(LibFcn):
    name = prefix + "ln1p"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27180
    def __call__(self, state, scope, pos, paramTypes, x):
        if x < -1.0:
            return float("nan")
        elif x == -1.0:
            return float("-inf")
        else:
            return math.log(1 + x)
provide(Ln1p())

class Round(LibFcn):
    name = prefix + "round"
    sig = Sigs([Sig([{"x": P.Float()}], P.Int()),
                Sig([{"x": P.Double()}], P.Long())])
    errcodeBase = 27190
    def __call__(self, state, scope, pos, paramTypes, x):
        return int(checkForOverflow(paramTypes[-1], math.floor(x + 0.5), self.errcodeBase + 0, self.errcodeBase + 1, self.name, pos))
provide(Round())

class RInt(LibFcn):
    name = prefix + "rint"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27200
    def __call__(self, state, scope, pos, paramTypes, x):
        if math.isinf(x) or math.isnan(x):
            return x
        up, down = math.ceil(x), math.floor(x)
        if up - x < x - down:
            return up
        elif up - x > x - down:
            return down
        elif int(up) % 2 == 0:
            return up
        else:
            return down
            
provide(RInt())

class Signum(LibFcn):
    name = prefix + "signum"
    sig = Sig([{"x": P.Double()}], P.Int())
    errcodeBase = 27210
    def __call__(self, state, scope, pos, paramTypes, x):
        if math.isnan(x):
            return 0
        elif x == 0:
            return 0
        elif x > 0:
            return 1
        else:
            return -1
provide(Signum())

class Sin(LibFcn):
    name = prefix + "sin"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27220
    def __call__(self, state, scope, pos, paramTypes, x):
        if math.isinf(x) or math.isnan(x):
            return float("nan")
        else:
            return math.sin(x)
provide(Sin())

class SinH(LibFcn):
    name = prefix + "sinh"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27230
    def genpy(self, paramTypes, args, pos):
        return "math.sinh({0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return math.sinh(x)
provide(SinH())

class Sqrt(LibFcn):
    name = prefix + "sqrt"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27240
    def __call__(self, state, scope, pos, paramTypes, x):
        try:
            return math.sqrt(x)
        except ValueError:
            return float("nan")
provide(Sqrt())

class Tan(LibFcn):
    name = prefix + "tan"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27250
    def __call__(self, state, scope, pos, paramTypes, x):
        if math.isinf(x) or math.isnan(x):
            return float("nan")
        else:
            return math.tan(x)
provide(Tan())

class TanH(LibFcn):
    name = prefix + "tanh"
    sig = Sig([{"x": P.Double()}], P.Double())
    errcodeBase = 27260
    def genpy(self, paramTypes, args, pos):
        return "math.tanh({0})".format(*args)
    def __call__(self, state, scope, pos, paramTypes, x):
        return math.tanh(x)
provide(TanH())

#################################################################### special functions

# class SpecialErf(LibFcn):
#     name = prefix + "special.erf"
#     sig = Sig([{"x": P.Double()}], P.Double())
#     a1 =  0.254829592
#     a2 = -0.284496736
#     a3 =  1.421413741
#     a4 = -1.453152027
#     a5 =  1.061405429
#     p  =  0.3275911
#     def __call__(self, state, scope, pos, paramTypes, x):
#         try:
#             return math.erf(x)
#         except AttributeError:
#             sign = -1.0 if x < 0 else 1.0
#             x = abs(x)
#             t = 1.0 / (1.0 + x * self.p)
#             y = 1.0 - (((((self.a5*t + self.a4)*t + self.a3)*t + self.a2)*t + self.a1)*t * math.exp(-x**2))
#             return sign * y    # erf(-x) = -erf(x)

# provide(SpecialErf())

