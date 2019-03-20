#!/bin/sh
lib=./lib/*
javafile=./src/Client/creatSub.java
outpath=./bin
javac -cp $lib $javafile -d $outpath
echo "build complete"