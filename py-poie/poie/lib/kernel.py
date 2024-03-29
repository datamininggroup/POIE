
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
from poie.util import callfcn, div
import poie.P as P
from poie.lib.array import argLowestN
from poie.lib.core import powLikeJava

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "m.kernel."

#################################################################### 
def dot(x, y, code, fcnName, pos):
    if len(x) != len(y):
        raise PFARuntimeException("arrays must have same length", code, fcnName, pos)
    else:
        return sum([i*j for i, j in zip(x,y)])

class RBF(LibFcn):
    name = prefix + "rbf"
    sig = Sig([{"x": P.Array(P.Double())}, 
               {"y": P.Array(P.Double())},
               {"gamma": P.Double()}], P.Double())
     
    errcodeBase = 23010
    def __call__(self, state, scope, pos, paramTypes, x, y, gamma):
        if len(x) != len(y):
            raise PFARuntimeException("arrays must have same length", self.errcodeBase + 0, self.name, pos)
        diff = [abs(i - j)**2 for i,j in zip(x,y)]
        return math.exp(-gamma*sum(diff))
provide(RBF())

class Linear(LibFcn):
    name = prefix + "linear"
    sig = Sig([{"x": P.Array(P.Double())}, 
               {"y": P.Array(P.Double())}], P.Double())
    errcodeBase = 23000
    """TDH Added spases"""
    def __call__(self, state, scope, pos, paramTypes, x, y):
	    return dot(x, y, self.errcodeBase + 0, self.name, pos)
provide(Linear())

class Poly(LibFcn):
    name = prefix + "poly"
    sig = Sig([{"x": P.Array(P.Double())}, 
               {"y": P.Array(P.Double())},
               {"gamma": P.Double()},
               {"intercept": P.Double()},
               {"degree": P.Double()}], P.Double())
    errcodeBase = 23020
    def __call__(self, state, scope, pos, paramTypes, x, y, gamma, intercept, degree):
	    return powLikeJava(gamma*dot(x, y, self.errcodeBase + 0, self.name, pos) + intercept, degree)
provide(Poly())

class Sigmoid(LibFcn):
    name = prefix + "sigmoid"
    sig = Sig([{"x": P.Array(P.Double())}, 
               {"y": P.Array(P.Double())},
               {"gamma": P.Double()},
               {"intercept": P.Double()}], P.Double())
    errcodeBase = 23030
    def __call__(self, state, scope, pos, paramTypes, x, y, gamma, intercept):
	    return math.tanh(gamma * dot(x, y, self.errcodeBase + 0, self.name, pos) + intercept)
provide(Sigmoid())
