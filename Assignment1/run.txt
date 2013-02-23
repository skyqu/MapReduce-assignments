#!/bin/sh

git clone git://github.com/lintool/Cloud9.git
hadoop fs -put data/bible+shakes.nopunc.gz

cp src/WordCountM.java Cloud9/src/dist/edu/umd/cloud9/example/simple
cd Cloud9
ant
etc/hadoop-cluster.sh edu.umd.cloud9.example.simple.WordCountM \
  -input bible+shakes.nopunc.gz -output wc -numReducers 5
cd
mkdir skyqu
cd skyqu
hadoop fs -get wc/part-r-*