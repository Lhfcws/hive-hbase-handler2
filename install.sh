#!/usr/bin/env bash
rm -rf hive-hbase-handler2
git clone https://github.com/Lhfcws/hive-hbase-handler2

cd hive-hbase-handler2
mvn clean package -U
mkdir dist
cp target

cp hive-hbase-handler2.jar ../dist/
chmod -R 755 dist