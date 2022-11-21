#!/bin/bash



hadoop fs -copyToLocal gs://title.principals.tsv
hadoop fs -copyToLocal gs://name.basics.tsv
hadoop fs -copyToLocal gs://proj1.jar
hadoop fs -copyToLocal gs://proj2.pig
hadoop fs -mkdir input
hadoop fs -copyFromLocal title.principals.tsv input/datasource1 
hadoop fs -copyFromLocal name.basics.tsv input/datasource4

echo " "
echo ">>>> usuwanie pozostałości po wcześniejszych uruchomieniach"

if $(hadoop fs -test -d ./output_mr3) ; then hadoop fs -rm -f -r ./output_mr3; fi

if $(hadoop fs -test -d ./output6) ; then hadoop fs -rm -f -r ./output6; fi

if $(hadoop fs -test -d ./project_files) ; then hadoop fs -rm -f -r ./project_files; fi

if $(test -d ./output6) ; then rm -rf ./output6; fi


echo " "
echo ">>>> kopiowanie skryptów, plików jar i wszystkiego co musi być dostępne w HDFS do uruchomienia projektu"
hadoop fs -mkdir project_files

hadoop fs -copyFromLocal proj1.jar project_files
hadoop fs -copyFromLocal proj1.pig project_files







echo " "
echo ">>>> uruchamianie zadania MapReduce - przetwarzanie (2)"
hadoop jar proj1.jar CompositeValueJob input/datasource1 output_mr3




echo " "
echo ">>>> uruchamianie skryptu Hive/Pig - przetwarzanie (5)"
export PIG_CLASSPATH=/etc/hadoop/conf.empty:/etc/tez/conf
pig -x tez -f proj2.pig -param inputMovies='output_mr3' -param inputActors='input/datasource4' -param out='output6'

echo " "
echo ">>>> pobieranie ostatecznego wyniku (6) z HDFS do lokalnego systemu plików"
mkdir -p ./output6
hadoop fs -copyToLocal output6/* ./output6

echo " "
echo " "
echo " "
echo " "
echo ">>>> prezentowanie uzyskanego wyniku (6)"
#ls output6
cat ./output6/*
