# flink-example


##  example from
Most of examples are from [Flink](http://github.com/apache/flink)


##  RocksDBTest
The profile are from [Flink](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/contrib/streaming/state/PredefinedOptions.html)    
The test result is [here](./RocksDBTest/Test.md)



##  rocksdb test persistent

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic msg-1k

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic msg-10k

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic msg-100k
```