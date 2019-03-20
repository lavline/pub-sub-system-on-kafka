#!/bin/sh
lib=./lib/*
javafile=./src/core/OpIndex.java
outpath=./bin
javac -cp $lib $javafile -d $outpath
echo "build complete"
