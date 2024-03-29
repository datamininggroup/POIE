
# Copyright (C) 2021 Data Mining Group
#
#
#
# This file is part of PFA Open Inference Engine (POIE)
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
from poie.signature import Lifespan
from poie.signature import PFAVersion
from poie.datatype import *
from poie.errors import *
from poie.util import callfcn, div
import poie.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "model.svm."

####################################################################
class Score(LibFcn):
    name = prefix + "score"
    sig = Sig([{"datum": P.Array(P.Double())},
               {"model": P.WildRecord("L",
                 {"const": P.Double(),
                  "posClass": P.Array(P.WildRecord("M", {"supVec": P.Array(P.Double()), "coeff" : P.Double()})),
                  "negClass": P.Array(P.WildRecord("N", {"supVec": P.Array(P.Double()), "coeff" : P.Double()}))})},
	       {"kernel": P.Fcn([P.Array(P.Double()), P.Array(P.Double())], P.Double())}
               ], P.Double())
    errcodeBase = 12000
    def __call__(self, state, scope, pos, paramTypes, datum, model, kernel):
        const    = model["const"]
        negClass = model["negClass"]
        posClass = model["posClass"]
        if len(negClass) == 0 and len(posClass) == 0:
            raise PFARuntimeException("no support vectors", self.errcodeBase + 0, self.name, pos)
        negClassScore = 0.0
        for sv in negClass:
            supVec = sv["supVec"]
            if len(supVec) != len(datum):
                raise PFARuntimeException("support vectors must have same length as datum", self.errcodeBase + 1, self.name, pos)
            coeff  = sv["coeff"]
            negClassScore += callfcn(state, scope, kernel, [supVec, datum])*coeff
        posClassScore = 0.0
        for sv in posClass:
            supVec = sv["supVec"]
            if len(supVec) != len(datum):
                raise PFARuntimeException("support vectors must have same length as datum", self.errcodeBase + 1, self.name, pos)
            coeff  = sv["coeff"]
            posClassScore += callfcn(state, scope, kernel, [supVec, datum])*coeff
        return negClassScore + posClassScore + const

provide(Score())
