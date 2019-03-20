#!/bin/sh
lib=./lib/*
javafile=./src/Client/SubConsumer.java
outpath=./bin
javac -cp $lib $javafile -d $outpath
echo "build complete"
