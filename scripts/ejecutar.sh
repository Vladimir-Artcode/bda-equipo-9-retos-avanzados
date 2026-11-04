#!/bin/bash
# Se ejecuta DENTRO del contenedor
export HADOOP_HOME=/opt/hadoop-3.3.6
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$PATH:$HADOOP_HOME/bin

cd ~/tienda
mkdir -p classes
javac -classpath "$(hadoop classpath)" -d classes TiendaJoin.java
jar cf tienda.jar -C classes .
rm -rf salida
hadoop jar tienda.jar TiendaJoin datos salida
cat salida/part-r-00000
