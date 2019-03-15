package io.github.chenfh5

import java.util
import java.util.concurrent.TimeUnit

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.slf4j.LoggerFactory
import org.testng.Assert
import org.testng.annotations.{AfterClass, BeforeClass, Test}
import pl.allegro.tech.embeddedelasticsearch.{EmbeddedElastic, PopularProperties}

class SinkTest {
  private val LOG = LoggerFactory.getLogger(getClass)
  private val es_http_port = 9200

  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  private lazy val embeddedElastic = EmbeddedElastic.builder
    //    .withElasticVersion("6.6.2")
    .withInResourceLocation("elasticsearch-6.6.2.zip") // make sure the zip file exist in resources path, if not then use with version to download, and comment this line
    .withSetting(PopularProperties.HTTP_PORT, es_http_port)
    .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9300)
    .withSetting(PopularProperties.CLUSTER_NAME, "es662")
    .withStartTimeout(2, TimeUnit.MINUTES)
    .withEsJavaOpts("-Xms128m -Xmx512m")
    .build

  private lazy val config = {
    val props = new util.HashMap[String, String]()
    props.put(EsConnConfig.ES_HOST, "localhost")
    props.put(EsConnConfig.ES_PORT, es_http_port.toString)
    props.put(EsConnConfig.ES_AUTH_USER, "chenfh5")
    props.put(EsConnConfig.ES_AUTH_PWD, "ji32k7au4a83")
    props
  }

  @BeforeClass
  def setUp(): Unit = {
    LOG.info("this is the test begin={}", OwnUtils.getTimeNow())
    embeddedElastic.start()
  }

  @AfterClass
  def shut(): Unit = {
    Thread.sleep(60 * 1000) // 1 min
    embeddedElastic.stop()
    LOG.info("this is the test   end={}", OwnUtils.getTimeNow())
  }

  @Test(enabled = false)
  def testStart(): Unit = {
    val task = new EsSinkTask()
    task.start(config)

    val res = task.version()
    assert(res == "0.0.1")
  }

  @Test(enabled = true)
  def testPut(): Unit = {
    EsConnConfig.`es.bulk.action` = 1

    // start
    val connector = new EsSinkConn()
    val task = new EsSinkTask()
    connector.start(config)
    task.start(config)

    // gen record
    val topic = "kafka_es_sink" // es_index_name or es_alias_name
    val value = Serialization.write(Map("Age" -> 10, "City" -> "LA", "Name" -> "Python")) // json string, order is matter, since es output is order by schema in asc

    val sinkRecord = new SinkRecord(topic, 0, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, value, 0)
    val records = new util.ArrayList[SinkRecord]()
    records.add(sinkRecord)

    // put
    task.put(records)
    Thread.sleep(2 * 1000)
    LOG.info("this is the fetch all={}", embeddedElastic.fetchAllDocuments(topic))
    Assert.assertTrue(embeddedElastic.fetchAllDocuments(topic).contains(value))
    task.stop()
    connector.stop()
  }

}
