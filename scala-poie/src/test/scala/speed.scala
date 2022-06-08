// Copyright 2021 Data Mining Group
//
// This file is part of POIE
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License

package test.scala.speed

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import org.datamininggroup.poie.ast._
import org.datamininggroup.poie.data._
import org.datamininggroup.poie.errors._
import org.datamininggroup.poie.jvmcompiler._
import org.datamininggroup.poie.reader._
import org.datamininggroup.poie.yaml._
import test.scala._

@RunWith(classOf[JUnitRunner])
class SpeedSuite extends FlatSpec with Matchers {
  "tree test" must "run a lot of data through a tree" taggedAs(Speed) in {
    val engine = PFAEngine.fromJson(getClass.getResourceAsStream("/resources/hipparcos_numerical_10.pfa")).head

    val dataset =
      (for (line <- new java.util.Scanner(getClass.getResourceAsStream("/resources/hipparcos_numerical.csv")).useDelimiter("\\n")) yield {
        val words = line.split(",")
        engine.jsonInput(s"""{
    "ra": ${words(0)},
    "dec": ${words(1)},
    "dist": ${words(2)},
    "mag": ${words(3)},
    "absmag": ${words(4)},
    "x": ${words(5)},
    "y": ${words(6)},
    "z": ${words(7)},
    "vx": ${words(8)},
    "vy": ${words(9)},
    "vz": ${words(10)},
    "spectrum": "${words(11)}"
}""")
      }).toList

    val datasetSize = dataset.size

    dataset.foreach(engine.action(_))
    dataset.foreach(engine.action(_))
    dataset.foreach(engine.action(_))

    var cumulative = 0.0
    for (iteration <- 0 until 50) {
      val before = System.currentTimeMillis
      dataset.foreach(engine.action(_))
      val after = System.currentTimeMillis

      cumulative += after - before
      println(s"""${cumulative / 1000.0}, ${(iteration + 1) * datasetSize}""")
    }

  }

  "tree test" must "run a lot of data through a forest" taggedAs(Speed) in {
    println("loading engine")
    val before = System.currentTimeMillis
    val engine = PFAEngine.fromJson(new java.util.zip.GZIPInputStream(getClass.getResourceAsStream("/resources/hipparcos_segmented_10.pfa.gz"))).head
    val after = System.currentTimeMillis
    println("done", (after - before))

    val dataset =
      (for (line <- new java.util.Scanner(getClass.getResourceAsStream("/resources/hipparcos_numerical.csv")).useDelimiter("\\n")) yield {
        val words = line.split(",")
        engine.jsonInput(s"""{
    "ra": ${words(0)},
    "dec": ${words(1)},
    "dist": ${words(2)},
    "mag": ${words(3)},
    "absmag": ${words(4)},
    "x": ${words(5)},
    "y": ${words(6)},
    "z": ${words(7)},
    "vx": ${words(8)},
    "vy": ${words(9)},
    "vz": ${words(10)},
    "spectrum": "${words(11)}"
}""")
      }).toList

    println("loaded dataset")

    val datasetSize = dataset.size

    dataset.foreach(engine.action(_))

    println("first foreach")

    dataset.foreach(engine.action(_))
    dataset.foreach(engine.action(_))

    var cumulative = 0.0
    for (iteration <- 0 until 50) {
      val before = System.currentTimeMillis
      dataset.foreach(engine.action(_))
      val after = System.currentTimeMillis

      cumulative += after - before
      println(s"""${cumulative / 1000.0}, ${(iteration + 1) * datasetSize}""")
    }

  }

}
