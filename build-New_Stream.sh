#!/bin/sh
lib=./lib/*
javafile=./src/core/New_Stream.java
outpath=./bin
javac -cp $lib $javafile -d $outpath
echo "build complete"
