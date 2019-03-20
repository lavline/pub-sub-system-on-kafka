#!/bin/sh
lib=./lib/*
javafile=./src/core/Stream_TAMA.java
outpath=./bin
javac -cp $lib $javafile -d $outpath
echo "build complete"
