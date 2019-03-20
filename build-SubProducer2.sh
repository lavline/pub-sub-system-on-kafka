#!/bin/sh
lib=./lib/*
javafile=./src/Client/SubProducer2.java
outpath=./bin
javac -cp $lib $javafile -d $outpath
echo "build complete"