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

import math

from poie.fcn import Fcn
from poie.fcn import LibFcn
from poie.signature import Sig
from poie.signature import Sigs
from poie.datatype import *
from poie.util import div
from poie.errors import PFARuntimeException
import poie.P as P

provides = {}
def provide(fcn):
    provides[fcn.name] = fcn

prefix = "stat.change."

class UpdateTrigger(LibFcn):
    name = prefix + "updateTrigger"
    sig = Sig([{"predicate": P.Boolean()}, {"history": P.WildRecord("A", {"numEvents": P.Int(), "numRuns": P.Int(), "currentRun": P.Int(), "longestRun": P.Int()})}], P.Wildcard("A"))
    errcodeBase = 37000
    def __call__(self, state, scope, pos, paramTypes, predicate, history):
        numEvents = history["numEvents"]
        numRuns = history["numRuns"]
        currentRun = history["currentRun"]
        longestRun = history["longestRun"]

        if numEvents < 0 or numRuns < 0 or currentRun < 0 or longestRun < 0:
            raise PFARuntimeException("counter out of range", self.errcodeBase + 0, self.name, pos)

        if predicate:
            numEvents += 1
            if currentRun == 0:
                numRuns += 1
            currentRun += 1
            if currentRun > longestRun:
                longestRun = currentRun
        else:
            currentRun = 0

        return dict(history, numEvents=numEvents, numRuns=numRuns, currentRun=currentRun, longestRun=longestRun)
provide(UpdateTrigger())

class ZValue(LibFcn):
    name = prefix + "zValue"
    sig = Sigs([Sig([{"x": P.Double()}, {"meanVariance": P.WildRecord("A", {"mean": P.Double(), "variance": P.Double()})}], P.Double()),
                Sig([{"x": P.Double()}, {"meanVariance": P.WildRecord("A", {"count": P.Double(), "mean": P.Double(), "variance": P.Double()})}, {"unbiased": P.Boolean()}], P.Double())])

    errcodeBase = 37010
    def __call__(self, state, scope, pos, paramTypes, x, meanVariance, *args):
        mean = meanVariance["mean"]
        variance = meanVariance["variance"]
        if variance >= 0.0:
            sigma = math.sqrt(variance)
        else:
            sigma = float("nan")
        if len(args) == 0:
            return div(x - mean, sigma)
        else:
            unbiased, = args
            count = meanVariance["count"]
            if count / (count - 1.0) >= 0.0:
                correction = math.sqrt((count) / (count - 1.0))
            else:
                correction = float("nan")
            if unbiased:
                return div(x - mean, sigma) * correction
            else:
                return div(x - mean, sigma)
provide(ZValue())

class UpdateCUSUM(LibFcn):
    name = prefix + "updateCUSUM"
    sig = Sig([{"logLikelihoodRatio": P.Double()}, {"last": P.Double()}, {"reset": P.Double()}], P.Double())
    errcodeBase = 37020
    def __call__(self, state, scope, pos, paramTypes, logLikelihoodRatio, last, reset):
        out = logLikelihoodRatio + last
        if out > reset:
            return out
        else:
            return reset
provide(UpdateCUSUM())
