import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}

/**
 *  @author Adam-Ma 
 *  @date 2022/4/27 9:20
 *  @Project spark-realtime-1118
 *  @email Adam_Ma520@outlook.com
 *  @phone 18852895353
 */
/**
*  创建 ES 客户端
 */
object ESTest {
  def main(args: Array[String]): Unit = {
    val restHighLevelClient: RestHighLevelClient = createESClient()

    println(restHighLevelClient)
    closeESClient(restHighLevelClient)
  }

  // 声明 ES 客户端对象
  val esClient : RestHighLevelClient = null

  // 创建 ES 客户端对象
  def createESClient(): RestHighLevelClient ={
//    val clientBuilder = new RestClientBuilder()   // 一般想法是直接去new ，但是对于 Builder 这种的来说，都是提供了相应的 builder 创建方法
    val hosts: HttpHost = new HttpHost("hadoop102",9200)
    val clientBuilder: RestClientBuilder = RestClient.builder(hosts)
    val restHighLevelClient = new RestHighLevelClient(clientBuilder)
    restHighLevelClient
  }

  // 关闭 ES 客户端
  def closeESClient(restHighLevelClient : RestHighLevelClient): Unit ={
    if (restHighLevelClient != null) {
      restHighLevelClient.close()
    }
  }

}
