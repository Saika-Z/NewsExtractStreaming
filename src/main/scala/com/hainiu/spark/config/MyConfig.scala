package com.hainiu.spark.config

object MyConfig {
  //xpath配置文件存放的位置
  final val XPATH_INFO_DIR: String = "hdfs://ns1/user/zhaowenqing20/xpath_cache_file"

  //更新xpath配置文件所在的地址
  final val UPDATE_XPATH_INTERVAL:Long = 10000L

  //hbase 表名
  final val HBASE_TABLE_NAME:String = "zhaowenqing01:context_extract01"

  //mysql 配置
  final val MYSQL_CONFIG:Map[String,String] = Map("url" -> "jdbc:mysql://nn2.hadoop","username"-> "hainiu","password"->"12345678" )

  //redis 配置
  final val REDIS_CONFIG:Map[String,String] = Map("host" -> "nn1.hadoop","port" -> "6379","time" -> "10000")

  //kafka的组
  final val KAFKA_GROUP:String = "lishuai6"

  //kafka的topic
  final val KAFKA_TOPIC:String = "hainiu_html"

  //kafka的broker
  final val KAFKA_BROKER:String = "s1.hadoop:9092,s2.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092"

  //kafka的offset读取位置
  final val KAFKA_OFFSET_POSITION:String = "earliest"

  //ES的host
  final val ES_HOST:String = "s1.hadoop"

  //ES的端口
  final val ES_PORT:String = "9200"

  //ES的index和type
  final val ES_INDEX_TYPE:String = "c20_zhaowq_index/giaos"

  //ZK配置
  final val ZOOKEEPER:String = "nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181"

}
