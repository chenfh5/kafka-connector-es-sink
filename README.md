# es sink
> generate doc from kafka producer, then serialized and put into queue, then deserialized from queue and sink into es

## version
- es v6.6.1
- kafka v0.10.2.0

## deploy
- When a connector is first submitted to the cluster, the workers rebalance the full set of connectors in the cluster and their tasks so that each worker has approximately the same amount of work

## Ref
- [kafka connect doc](http://kafka.apache.org/documentation.html#connect)
- [one github proj0](https://github.com/jeff-svds/kafka-connect-opentsdb)
- [one github proj1](https://github.com/hannesstockner/kafka-connect-elasticsearch)
- [one github proj2](https://github.com/renukaradhya/Kafka-Connect-ElasticSearch)
- [one github proj3](https://github.com/DataReply/kafka-connect-elastic-search-sink)
- [开发部署](https://my.oschina.net/hnrpf/blog/1555915)
- [开发部署2](https://cloud.tencent.com/developer/article/1362324)
