#!/bin/sh
lib=./lib/*
javafile=./src/EventSender/EventProducer.java
outpath=./bin
javac -cp $lib $javafile -d $outpath
echo "build complete"
