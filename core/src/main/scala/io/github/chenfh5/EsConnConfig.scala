package io.github.chenfh5

import java.util

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}


class EsConnConfig(originals: util.Map[String, String]) extends AbstractConfig(EsConnConfig.config, originals.asInstanceOf[util.Map[String, Object]]) {}

object EsConnConfig {
  val ES_HOST = "es.host"
  val ES_PORT = "es.port"
  val ES_AUTH_USER = "es.auth.user"
  val ES_AUTH_PWD = "es.auth.pwd"


  private lazy val ownConf = ConfigFactory.load("conf/ownConf.properties")
  var `es.bulk.action`: Int = ownConf.getInt("es.bulk.action")
  val `es.bulk.size`: Int = ownConf.getInt("es.bulk.size") // in mb
  val `es.bulk.concurr.req`: Int = ownConf.getInt("es.bulk.concurr.req")
  val `es.bulk.flush.interval`: Int = ownConf.getInt("es.bulk.flush.interval")
  val `es.bulk.backoff.time`: Int = ownConf.getInt("es.bulk.backoff.time")
  val `es.bulk.backoff.try`: Int = ownConf.getInt("es.bulk.backoff.try")
  val `es.type.name`: String = ownConf.getString("es.type.name")

  val config: ConfigDef = new ConfigDef()
    .define(ES_HOST, Type.STRING, "localhost", Importance.HIGH, "es host")
    .define(ES_PORT, Type.INT, 9200, Importance.HIGH, "es port")
    .define(ES_AUTH_USER, Type.STRING, "", Importance.HIGH, "es auth user name")
    .define(ES_AUTH_PWD, Type.STRING, "", Importance.HIGH, "es auth encode user password")
}


