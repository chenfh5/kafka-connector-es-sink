package io.github.chenfh5

import java.util

import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory

class EsSinkTask extends SinkTask {
  private val LOG = LoggerFactory.getLogger(getClass)
  private var client: EsClient = _

  override def start(map: util.Map[String, String]): Unit = {
    val config = new EsConnConfig(map)
    val esHost = config.getString(EsConnConfig.ES_HOST)
    val esPort = config.getInt(EsConnConfig.ES_PORT)
    val esAuthUser = config.getString(EsConnConfig.ES_AUTH_USER)
    val esAuthPwd = config.getString(EsConnConfig.ES_AUTH_PWD)
    client = EsClient(esHost, esPort, esAuthUser, esAuthPwd) // init es client
    LOG.info("this is the start(), esClient={}", client)
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    import scala.collection.JavaConverters._
    for (e <- records.asScala) {
      LOG.info("this is the processing record=%s".format(e))
      client.getBulk.add(new IndexRequest(e.topic(), EsConnConfig.`es.type.name`).source(e.value().toString, XContentType.JSON))
    }
  }

  override def stop(): Unit = client.get.close()

  override def version(): String = "0.0.1"
}
