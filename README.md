# es sink
> generate doc from kafka producer, then serialized and put into queue, then deserialized from queue and sink into es

## Version
- es v6.6.2
- kafka v0.10.2.0

## Quickstart
- run ut
- bulk mode
- topic name is es index name
- the left part (todo kafka producer)
- to used for multiple es cluster, use Map\[es_cluster_name, esClient\] to dispatch


## Deploy
- When a connector is first submitted to the cluster, the workers rebalance the full set of connectors in the cluster and their tasks so that each worker has approximately the same amount of work

## Reference
- [kafka connect doc](http://kafka.apache.org/documentation.html#connect)
- [one github proj](https://github.com/jeff-svds/kafka-connect-opentsdb)
- [one github proj1](https://github.com/hannesstockner/kafka-connect-elasticsearch)
- [one github proj2](https://github.com/renukaradhya/Kafka-Connect-ElasticSearch)
- [one github proj3](https://github.com/DataReply/kafka-connect-elastic-search-sink)
- [开发部署](https://my.oschina.net/hnrpf/blog/1555915)
- [开发部署2](https://cloud.tencent.com/developer/article/1362324)
- [ESTestCase](https://www.elastic.co/guide/en/elasticsearch/reference/current/unit-tests.html)

## TestCase
```
<groupId>io.confluent</groupId>
<artifactId>kafka-connect-elasticsearch</artifactId>
```
Here I use [embedded-elasticsearch](https://github.com/allegro/embedded-elasticsearch), in `confluentinc/kafka-connect-elasticsearch` It use [ESIntegTestCase](https://github.com/confluentinc/kafka-connect-elasticsearch/blob/v5.1.2/src/test/java/io/confluent/connect/elasticsearch/ElasticsearchSinkTestBase.java#L59).

Run ElasticsearchSinkTaskTest, then with these process,
1. ElasticsearchSinkTestBase extends ESIntegTestCase -> setUp() -> getPort() -> cluster() // 从ESIntegTestCase里面初始化一个默认集群
2. 从cluster()里面获取http port,然后传入JestElasticsearchClient,产生jest http client
3. ElasticsearchSinkTaskTest的testPutAndFlush(), 将`jest http client`传入task的start()来初始化task
4. 将record写入task的queue,put() -> write() -> tryWriteRecord() -> add() -> addLast() -> farmerTask多线程检查是否submitReady -> 如果ok(flush OR time OR size) -> submitBatch() -> BulkTask.executor.submit() -> call() -> execute() -> executeBulk() -> client.execute() -> prepareRequest() -> return Responese
