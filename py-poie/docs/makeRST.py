
import importlib
import inspect

modules = [
    "poie.datatype",
    "poie.errors",
    "poie.fcn",
    "poie.genpy",
    "poie.options",
    "poie.pfaast",
    "poie.P",
    "poie.prettypfa",
    "poie.reader",
    "poie.signature",
    "poie.util",
    "poie.version",
    "poie.producer.cart",
    "poie.producer.chain",
    "poie.producer.expression",
    "poie.producer.kmeans",
    "poie.producer.tools",
    "poie.producer.transformation",
    "poie.inspector.defs",
    "poie.inspector.jsongadget",
    "poie.inspector.main",
    "poie.inspector.parser",
    "poie.inspector.pfagadget",
    ]

modules = {name: importlib.import_module(name) for name in modules}

documented = []
for moduleName, module in list(modules.items()):
    for objName in dir(module):
        obj = getattr(module, objName)
        if not objName.startswith("_") and callable(obj) and obj.__module__ == moduleName:
            print(objName, obj)
            documented.append(moduleName + "." + objName)
            if inspect.isclass(obj):
                open(moduleName + "." + objName + ".rst", "w").write('''
{0}
{1}

.. autoclass:: {0}
    :members:
    :undoc-members:
    :show-inheritance:
'''.format(moduleName + "." + objName, "=" * (len(moduleName) + len(objName) + 1)))
            else:
                open(moduleName + "." + objName + ".rst", "w").write('''
{0}
{1}

.. autofunction:: {0}
'''.format(moduleName + "." + objName, "=" * (len(moduleName) + len(objName) + 1)))

documented.sort()

open("index.rst", "w").write('''
PyPoie |version|
===============

:ref:`genindex`

.. toctree::
   :maxdepth: 2

''' + "\n".join("   " + x for x in documented) + "\n")
