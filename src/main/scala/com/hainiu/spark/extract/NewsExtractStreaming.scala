package com.hainiu.spark.extract

import java.io.BufferedReader
import java.util.Date

import org.apache.kafka.common.TopicPartition
import java.util.Map
import java.{lang, util}
import java.net.URL

import com.hainiu.spark.brodcast.{BroadcastWapper, MapAccumulator}
import com.hainiu.spark.config.MyConfig
import com.hainiu.spark.db.HainiuJedisConnectionPool
import com.hainiu.spark.utils.{DBUtil, FileUtil, JavaUtil, KafkaUtil, Util}
import com.hainiu.spark.utils.extractor.HtmlContentExtractor
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.jsoup.Jsoup
import redis.clients.jedis.{Jedis, Transaction}

import scala.collection.convert.wrapAsJava.mutableSeqAsJavaList
import scala.collection.mutable

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, ListBuffer}
import scala.util.control.Breaks._


/**
  * 偏移量保存在zk中
  * 不使用DStream的transform等其他算子
  * 将DStream数据处理方式转成spark-core的数据处理方式
  * SparkStreaming程序长时间中断，再次消费时kafka中数据已过时，上次记录消费的offset已丢失的问题
  */
class NewsExtractStreaming
object NewsExtractStreaming {

  def main(args: Array[String]): Unit = {
    //指定组名
    val group = MyConfig.KAFKA_GROUP
    //创建SparkConf
    val conf = new SparkConf().setAppName("newsextractstreaming").setMaster("local[*]")
    //使用ES的java api 手动创建
    conf.set("es.nodes",MyConfig.ES_HOST)
    conf.set("es.port",MyConfig.ES_PORT)

    //创建SparkStreaming，设置间隔时间
    val ssc = new StreamingContext(conf,Durations.seconds(5))

    //指定topic名字
    val topic = MyConfig.KAFKA_TOPIC
    //指定kafka的broker地址，SparkStream的Task直连到kafka分区上，底层用API消费，效率更高
    val brokerList = MyConfig.KAFKA_BROKER
    //指定zk地址，更新消费的偏移量时使用，也可以用Redis和MySQL来记录偏移量
    val zkQuorum = MyConfig.ZOOKEEPER
    //SparkStreaming时使用的topic集合，可以同时消费多个topic
    val topics: Set [String] = Set(topic)
    //topic 在zk里的数据路径，用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group,topic)
    //得到zk中的数据路径 比如 "/consumers/${group}/offset/${topic}"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //kafka参数
    val kafkaParams = mutable.Map(
      "bootstrap.servers" -> brokerList,
      "group.id" -> group,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: lang.Boolean),
      //earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      //latest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的的offset时，消费新产生的该分区下抛出异常
      //none topic各分区都存在已提交offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
      //"auto.offset.reset" -> MyConfig.KAFKA_OFFSET_POSITION
      "auto.offset.reset" -> "latest"
    )

    //定义一个空的kafkaStream，之后根据是否有历史偏移进行选择
    var kafkaStream:InputDStream[ConsumerRecord[String,String]] = null
    //创建zk客户端，可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(zkQuorum)
    print(zkClient)

    //*****************************************************************************************

    //判断zk中是否存在历史offset
    val zkExist: Boolean = zkClient.exists(s"$zkTopicPath")
    if(zkExist){
            val clusterEarliestOffsets:HashMap[Long,Long] = KafkaUtil.getPartitionOffset(topic)
//            val clusterEarliestOffsets: Map[Long, Long] = KafkaUtil.getPartitionOffset(topic)
      val nowOffsetMap:HashMap[TopicPartition,Long] = KafkaUtil.getPartitionOffsetZK(topic,zkTopicPath,clusterEarliestOffsets,zkClient)
      //通过kafkaUtils创建直连的DStream，并使用fromOffset中储存的历史偏移量来继续消费
      kafkaStream = KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe(topics,kafkaParams,nowOffsetMap))
    }else{
      //如果zk中没有该topic的历史offset，那就根据kafkaParam的配置使用最新(latest)或者最旧的(earliest)的offset
      kafkaStream = KafkaUtils.createDirectStream(ssc,PreferConsistent, Subscribe(topics,kafkaParams))
    }

    //创造5个自定义累加器,记录每个批次host的抽取情况
    val hostScanAccumulator = new MapAccumulator
    val hostFilteredAccumulator = new MapAccumulator
    val hostExtractAccumulator = new MapAccumulator
    val hostEmptyAccumulator = new MapAccumulator
    val hostNotMatchAccumulator = new MapAccumulator
    ssc.sparkContext.register(hostScanAccumulator)
    ssc.sparkContext.register(hostFilteredAccumulator)
    ssc.sparkContext.register(hostExtractAccumulator)
    ssc.sparkContext.register(hostEmptyAccumulator)
    ssc.sparkContext.register(hostNotMatchAccumulator)

    //将xpath配置做成广播变量，定时进行广播更新
    val xpathInfo = new BroadcastWapper[HashMap[String,HashMap[String,HashSet[String]]]](ssc,new HashMap[String,HashMap[String,HashSet[String]]])
    //通过rdd转换得到偏移量的范围
    var offsetRanges = Array[OffsetRange]()

    //记录xpath配置上次更新的时间
    //这个可以是本地变量的形式，因为这个变量只在driver上使用到了
    var lastUpdateTime = 0L

    //迭代DStream中的RDD，将每一个时间间隔对应的RDD拿出来，在driver端执行
    //在foreachRDD方法中开发spark-core
    kafkaStream.foreachRDD(kafkaRDD =>{
      if(!kafkaRDD.isEmpty()){
        //得到该Rdd对应的kafka消息的offset，该RDD是一个kafkaRDD，所以可以获得偏移量的范围
        //不使用transform可以直接在foreachRDD中得到这个RDD的偏移量，这种方法适用于DStream不经过任何转换
        //直接进行foreachRDD，因为如果transformation了那就不是KafkaRDD了，就不能强转成HasOffsetRanges了
        offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        //先进行广播变量的更新操作
        //foreachRDD是在driver上执行的，所以不需要耗费每个worker的执行时间
        //把xpathInfo.value.isEmpty放后面可以减少其被执行的次数
        if(System.currentTimeMillis() - lastUpdateTime >MyConfig.UPDATE_XPATH_INTERVAL||xpathInfo.value.isEmpty){
          //存储xpath配置的数据结构
          val xpathInfoUpdate:HashMap[String,HashMap[String,HashSet[String]]] = new HashMap[String,HashMap[String,HashSet[String]]]

          val fs = FileSystem.get(new Configuration())
          val fileStatuses:Array[FileStatus] = fs.listStatus(new Path(MyConfig.XPATH_INFO_DIR))

          //读取配置文件，将xpath信息存到xpathInfoUpdate

          for(fileStatus <- fileStatuses){
            breakable{
              val filePath = fileStatus.getPath
              if(filePath.toString.endsWith("_COPYING_")){
                break()
              }
              val reader:BufferedReader = FileUtil.getBufferedReader(filePath)

              var line:String = reader.readLine()
              while (line !=null){
                val strings = line.split("\t")

                //避免出现数组越界
                if (strings.length >= 3){
                  val host = strings(0)
                  val xpath = strings(1)
                  val xPathType = strings(2)

                  val hostXpathInfo = xpathInfoUpdate.getOrElseUpdate(host,new HashMap[String,HashSet[String]]())
                  //上面这行代码与下面两行代码相同
                  //val hostXpathInfo = xpathInfoUpdate.getOrElseUpdate(host,new HashMap[String,HashSet[String]])
                  //xpathInfoUpdate += host -> new HashMap[String,HashSet[String]]()

                  val hostTypeXpathInfo = hostXpathInfo.getOrElseUpdate(xPathType,new HashSet[String]())
                  hostTypeXpathInfo.add(xpath)
                }

                line = reader.readLine()
              }
              reader.close()

            }
          }
          //更新xpath配置的广播变量
          xpathInfo.update(xpathInfoUpdate,blocking = true)
          //更新xpath配置更新的时间广播变量
          lastUpdateTime = System.currentTimeMillis()
        }

        val dataRDD:RDD[(String,String,String,String,String,String)] = kafkaRDD.filter(cr =>{
//          val key: String = cr.key()
//          println("*" * 50 + key)
          val record:String = cr.value()
          val strings: Array[String] = record.split("\001")
          var isCorrect:Boolean = true
          if(strings.length ==3){
            val md5 = strings(0)
            val url = strings(1)
            val html = strings(2)
            val urlT = new URL(url)
            var host: String = urlT.getHost
            hostScanAccumulator.add(host -> 1L)
            val checkMd5 = DigestUtils.md5Hex(s"$url\001$html")
            if (!checkMd5.equals(md5)){
              hostFilteredAccumulator.add(host -> 1L)
              isCorrect = false
            }
          }else if(strings.length == 2){
            val url = strings(1)
            val utlT = new URL(url)
            val host:String = utlT.getHost
            hostScanAccumulator.add(host ->1L)
            hostFilteredAccumulator.add(host -> 1L)
            isCorrect = false
          }else{
            hostScanAccumulator.add("HOST_NULL" -> 1L)
            hostFilteredAccumulator.add("HOST_NULL" -> 1L)
            isCorrect = false
          }
          isCorrect
        }).mapPartitions(iter =>{
          //从广播变量中取得xpath信息
          val xpathMap:HashMap[String,HashMap[String,HashSet[String]]] = xpathInfo.value

          //缓存hbace记录
          val puts = new ListBuffer[Put]
          //缓存ES记录，用于返回
          val list = new ListBuffer[(String,String,String,String,String,String)]

          iter.foreach(f = cr => {
            val strings = cr.value().split("\001")
            val url: String = strings(1)
            val urlT = new URL(url)
            val host: String = urlT.getHost
            val domain: String = JavaUtil.getDomainName(urlT)
            val html: String = strings(2)

            val xpathMapT: Map[String, String] = HtmlContentExtractor.generateXpath(html)
            val failedRule = new ArrayBuffer[String]
            var trueRule = ""
            if (JavaUtil.isNotEmpty(xpathMapT)) {
              //              val value:Iterator[Map.Entry[String,String]] =
              val value: util.Iterator[Map.Entry[String, String]] = xpathMapT.entrySet().iterator()
              while (value.hasNext) {
                val entry: Map.Entry[String, String] = value.next()
                val key = entry.getKey
                if (key != null && !key.trim().equals("")) {
                  if (entry.getValue.equals(HtmlContentExtractor.CONTENT)) {
                    trueRule = key
                  } else {
                    failedRule += key
                  }
                }
              }
            }

            val redis: Jedis = HainiuJedisConnectionPool.getConnection()
            //把正反规则存到redis中，只有redis事务
            if (!trueRule.equals("") || !failedRule.isEmpty) {
              val transaction: Transaction = redis.multi()
              //切换数据库
              transaction.select(12)

              //正规则存总值和有序集合，总值累加key的前缀为total开头，反规则存自动排序的集合，key的前缀为txpath开头
              if (!trueRule.equals("")) {
                transaction.incr(s"C20_zhaowq:total:${host}")
                transaction.zincrby(s"C20_zhaowq:txpath:${host}", 1, trueRule)
              }
              //反规则存集合，key的前缀为fxpath开头
              if (!failedRule.isEmpty) {
                transaction.sadd(s"C20_zhaowq:fxpath:${host}", failedRule: _*)
              }
              transaction.exec()

            }
            redis.close()

            //正文抽取，拿到host对应的正反序列
           // print("-------------"+ host)

            if (xpathMap.contains(host)) {
              val hostXpathInfo: HashMap[String, HashSet[String]] = xpathMap(host)
              val hostPositiveXpathInfo: HashSet[String] = hostXpathInfo.getOrElse("true", new HashSet[String])
              val hostNegativeXpathInfo: HashSet[String] = hostXpathInfo.getOrElse("false", new HashSet[String])

              //抽取正文
              val doc = Jsoup.parse(html)

              //scala代码
              val content = Util.getContext(doc, hostPositiveXpathInfo, hostNegativeXpathInfo)

              if (content.trim.length >= 10) {
                val time = new Date().getTime
                val urlMd5: String = DigestUtils.md5Hex(url)
                list += ((url, host, content, html, domain, urlMd5))

                //hbase
                //create 'context_extract',
                //{NAME => 'i', VERSIONS => 1, BLOCKCACHE => true,COMPRESSION => 'SNAPPY'},
                //{NAME => 'c', VERSIONS => 1, BLOCKCACHE => true,COMPRESSION => 'SNAPPY'},
                //{NAME => 'h', VERSIONS => 1,COMPRESSION => 'SNAPPY'}

                val put = new Put(Bytes.toBytes(s"${host}_${Util.getTime(time, "yyyyMMddHHMMssSSSS")}_$urlMd5"))
                if (JavaUtil.isNotEmpty(url)) put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("url"), Bytes.toBytes(url))
                if (JavaUtil.isNotEmpty(domain)) put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("domain"), Bytes.toBytes(domain))
                if (JavaUtil.isNotEmpty(host)) put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("host"), Bytes.toBytes(host))
                if (JavaUtil.isNotEmpty(content)) put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("content"), Bytes.toBytes(content))
                if (JavaUtil.isNotEmpty(html)) put.addColumn(Bytes.toBytes("h"), Bytes.toBytes("html"), Bytes.toBytes(html))
                puts += put
                hostExtractAccumulator.add(host -> 1L)

              } else {
                //根据xpath匹配到正文长度小于10，说明这个xpath很可能不正确
                hostEmptyAccumulator.add(host -> 1L)
              }
            } else {
              //此host没有匹配的xpath规则
              hostNotMatchAccumulator.add(host -> 1L)
            }
          })

          //保存到Hbase
          val hbaseConf:Configuration = HBaseConfiguration.create()
          val connection:Connection = ConnectionFactory.createConnection(hbaseConf)
          val table:HTable = connection.getTable(TableName.valueOf(MyConfig.HBASE_TABLE_NAME)).asInstanceOf[HTable]
          table.put(puts)
          table.close
          connection.close()

          list.toIterator
        })
          //保存到es
        import org.elasticsearch.spark._
        dataRDD.mapPartitions(f =>{
          val date:String = Util.getTime(new Date().getTime,"yyyy-MM-dd HH:mm:ss")
          val r1 = new ListBuffer[mutable.Map[String,String]]
          for(t<- f){
            val url:String = t._1
            val host:String = t._2
            val content:String = t._3
            val domain:String = t._5
            val urlMd5:String = t._6
            val rm = mutable.Map("url" -> url,
              "url_md5" -> urlMd5,
              "host" -> host,
              "domain" -> domain,
              "date" -> date,
              "content" -> content)
            r1 += rm
          }
          r1.toIterator
        }).saveToEs(MyConfig.ES_INDEX_TYPE)

        val scanAccMap:HashMap[Any,Any] = hostScanAccumulator.value
        val scanNum = scanAccMap.foldLeft(0L)(_ + _._2.asInstanceOf[Long])

        val filteredAccMap:HashMap[Any,Any] = hostFilteredAccumulator.value
        val filteredNum = filteredAccMap.foldLeft(0L)(_ + _._2.asInstanceOf[Long])

        val extractAccMap:HashMap[Any,Any] = hostExtractAccumulator.value
        val extractNum = extractAccMap.foldLeft(0L)(_ + _._2.asInstanceOf[Long])

        val emptyAccMap:HashMap[Any,Any] = hostEmptyAccumulator.value
        val emptyNum = emptyAccMap.foldLeft(0L)(_ + _._2.asInstanceOf[Long])

        val notmatchAccMap:HashMap[Any,Any] = hostNotMatchAccumulator.value
        val notMatchNum = notmatchAccMap.foldLeft(0L)(_ + _._2.asInstanceOf[Long])

        //打印被批次的统计信息，可以在driver上看到
        println(s"last modify time:${Util.getCurrentTime}")
        println(s"report ====> scan: $scanNum," +
          s"filtered: $filteredNum, " +
          s"extract: $extractNum," +
          s"empty: $emptyNum," +
          s"noMatchXpath: $notMatchNum")

        //重置累加器
        hostScanAccumulator.reset()
        hostFilteredAccumulator.reset()
        hostExtractAccumulator.reset()
        hostEmptyAccumulator.reset()
        hostNotMatchAccumulator.reset()

        //将统计数据写入mysql
        DBUtil.insertIntoMysqlByJdbc(scanAccMap,filteredAccMap,extractAccMap,emptyAccMap,notmatchAccMap)

        //将offset存入zookeeper
        for(o <- offsetRanges){
          //   /consumers/zhaowenqing20/offsets/hainiu_zhaowenqing/0
          val zKPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          //  /consumers/zhaowenqing20/offsets/hainiu_zhaowenqing/888
          ZkUtils(zkClient,false).updatePersistentPath(zKPath,o.partition.toString)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
