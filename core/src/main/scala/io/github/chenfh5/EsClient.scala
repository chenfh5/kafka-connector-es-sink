package io.github.chenfh5

import java.util.function.BiConsumer

import io.github.chenfh5.EsConnConfig._
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}
import org.slf4j.LoggerFactory

case class EsClient(host: String, port: Int, authUser: String, authPW: String) {
  private val LOG = LoggerFactory.getLogger(getClass)

  private val client: RestHighLevelClient = {
    val credentialsProvider = new BasicCredentialsProvider()
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(authUser, OwnUtils.decode(authPW)))

    val restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, "http"))
      .setHttpClientConfigCallback((httpClientBuilder: HttpAsyncClientBuilder) => httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)))

    // cluster check
    val res = restHighLevelClient.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT)
    LOG.info("this is the setup cluster_name={}", res.getClusterName)

    restHighLevelClient
  }

  // @see https://stackoverflow.com/a/52061477/9108627
  // @see https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.6/java-rest-high-document-bulk.html#java-rest-high-document-bulk-processor
  // @see http://dmlcoding.com/15198741843316.html
  private val bulkProcessor = {
    val listener = new BulkProcessor.Listener {
      override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {}

      override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {}

      override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = LOG.info("bulk fail={}", failure.getMessage)
    }

    val bulkConsumer: BiConsumer[BulkRequest, ActionListener[BulkResponse]] =
      (bulkRequest: BulkRequest, _: ActionListener[BulkResponse]) => client.bulk(bulkRequest, RequestOptions.DEFAULT) // sync or async


    val bp = BulkProcessor.builder(bulkConsumer, listener)
      .setBulkActions(`es.bulk.action`)
      .setBulkSize(new ByteSizeValue(`es.bulk.size`, ByteSizeUnit.MB))
      .setFlushInterval(TimeValue.timeValueMinutes(EsConnConfig.`es.bulk.flush.interval`)) // make it disable
      .setConcurrentRequests(`es.bulk.concurr.req`)
      .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueSeconds(`es.bulk.backoff.time`), `es.bulk.backoff.try`))
      .build()

    LOG.info("this is the setBulkActions={}, setBulkSize={},", `es.bulk.action`, `es.bulk.size`)
    bp
  }

  def get: RestHighLevelClient = if (client == null) throw new RuntimeException("client is not init") else client

  def getBulk: BulkProcessor = if (bulkProcessor == null) throw new RuntimeException("bulkProcessor is not init") else bulkProcessor

}