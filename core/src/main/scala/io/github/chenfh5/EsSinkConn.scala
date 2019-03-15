package io.github.chenfh5

import java.util

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.slf4j.LoggerFactory

class EsSinkConn extends SinkConnector {
  private val LOG = LoggerFactory.getLogger(getClass)
  private val property = new util.HashMap[String, String]()

  override def version(): String = "0.0.1"

  override def start(map: util.Map[String, String]): Unit = property.putAll(map)

  override def taskClass(): Class[_ <: Task] = classOf[EsSinkTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    LOG.info(s"this is the taskConfigs(), property={}", property)

    // init
    val configs = new util.ArrayList[util.Map[String, String]]()
    val config = new util.HashMap[String, String]()
    config.putAll(property)

    // loop
    for (_ <- 0 until maxTasks) configs.add(config)

    // res
    configs
  }

  override def stop(): Unit = {} // no implement

  override def config(): ConfigDef = EsConnConfig.config
}
