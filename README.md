# Hive-Hbase-Handler2 (Developing)

- push down filters
- remote cluster support (hbase and hive in the different clusters)

## 1. push down filters


### Sargable

Sargable = Search ARGument ABLE

+ [Sargable Intro (Chinese)](http://www.cnblogs.com/lhfcws/p/6611830.html)
+ [Sargable Wikipedia](https://en.wikipedia.org/wiki/Sargable)

#### Current sargable operators (1.2.1.0)

> =,!=,>,<,>=,<=,between,is_null,rlike


## 2. remote cluster support 

HBase and Hive can be in the different clusters. We call this kind of hive table as a remote table.

The project can only support the remote table as a external table and it will not be managed by the metastore.
 
As a trick, the project use a convention that a table ending With "_" will be considered as a remote table, due to no table properties can be achieved in HiveMetaHook.
 
A config in TBLPROPERTIES is added for this feature: `"hbase.remote.conf.file"="/local/hdfs/path/to/remote_cluster_hbase-site.xml"` , it tells the hive services which hbase it should connect.  


## Usage

Check the pom.xml if the versions of hadoop/hbase/etc. suit your cluster, modify pom.xml and re-package if you want.

Put the jar under `${HIVE_HOME}/lib/` .
 
## TODO features:
1. hive complexed datatype optimized support


## Remote install

~~~bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/Lhfcws/hive-hbase-handler2/master/install.sh)"
~~~