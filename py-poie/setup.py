
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

from setuptools import setup

import poie.version

### To run the tests:
# python setup.py test

### To install on a machine:
# sudo python setup.py install

### To install in a home directory (~/lib):
# python setup.py install --home=~

setup(name="poie",
      version=poie.version.__version__,
      author="Data Mining Group",
      author_email="info@dmg.org",
      packages=["poie",
                "poie.producer",
                "poie.lib",
                "poie.lib.model",
                "poie.lib.prob",
                "poie.lib.stat",
                "poie.pmml",
                "poie.inspector"],
      scripts = ["scripts/pfainspector", "scripts/pfachain", "scripts/pfaexternalize", "scripts/pfarandom", "scripts/pfasize"],
      description="Python implementation of Portable Format for Analytics (PFA): producer, converter, and consumer.",
      test_suite="test",
      install_requires=["avro >= 1.7.7", "ply == 3.4"],
      tests_require=["avro >= 1.7.7", "ply == 3.4", "PyYAML >= 3.10", "numpy >= 1.6.1", "pytz == 2015.4"],
      )
