Sqoop
==========

This is a patched version of Apache Sqoop(TM) v1.4.2

The patches added are described in the commit messages.

**Building**

Sqoop is compiled with Ant.

Type _ant_ to compile all java sources. You can then run Sqoop with _bin/sqoop_.

By default, Sqoop is build versus Hadoop version 0.23. To change version, the parameter _hadoopversion_ must be used.

For example, to compile versus Hadoop 2.0, type the following:

_ant -Dhadoopversion=200_

For further details, see the _COMPILING.txt_ file.

**Version**

The version of Sqoop is stored in the _version_ property of _build.xml_.
It has been changed from the original version to keep track of the changes.
