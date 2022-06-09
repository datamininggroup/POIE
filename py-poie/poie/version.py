
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

__version_info__ = (0,9,0)

__version__ = ".".join(map(str, __version_info__))

pypoieVersion = __version__    # the poie version is for informational purposes only; doesn't affect behavior

defaultPFAVersion = "0.8.1"   # the PFA version determines how poie will interpret PFA documents (can be overridden)
                              # must always be in the form [1-9][0-9]*\.[1-9][0-9]*\.[1-9][0-9]*
