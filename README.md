# POIE
PFA Open Inference Engine.  Reference implementations of the PFA specification

========

The [Portable Format for Analytics (PFA)](http://dmg.org/pfa) is a specification for scoring engines: event-based processors that perform predictive or analytic calculations. It is a common language to help smooth the transition from statistical model development to large-scale and/or online production. For a model expressed as PFA to be run against data, an application is required.

**scala-poie** The Scala implementation of the PFA Open Inference Engine (POIE) is a complete implementation of PFA for the Java Virtual Machine (JVM).  Scala-poie is designed as a library to be embedded in applications or used as a scoring engine container.

**py2-poie** A Python 2.7 implementation of PFA for Python. Scala-poie and py-poie both execute the same scoring engines, but while speed and portability are the scala implementation's focus, the Python implementation's focus is on model development. Included are standard model producers, a PrettyPFA parser for easier editing, a PFA-Inspector command line for interactive analysis of a PFA document, and many other tools and scripts.

**py-poie** is a updated version Python version that is limited to UTF-8 strings and is under active development.

See the [DMG site](http://dmg.org) for more information.
