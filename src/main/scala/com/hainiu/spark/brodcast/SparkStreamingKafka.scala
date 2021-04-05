package com.hainiu.spark.brodcast

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable

object SparkStreamingKafka {
  def main(args: Array[String]): Unit = {
    // 创建kafka直连流，没有receiver接收，所以可以用local[1]
    // 注意：分配的CPU核数最好要大于等于kafka的分区数，这样可以每个cpu核对应一个分区
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkStreamingKafka")
    val ssc = new StreamingContext(sparkConf, Durations.seconds(5))

    val topics = "hainiu_sk"

    // 消费kafka的配置
    // sparkStreaming 直连kafka的分区，kafka有多少分区，sparkStreaming流就有多少分区
    // 直连kafka，消费kafka时直接读broker，消费的offset也维护到broker（topic：__consumer_offsets）
    val kafkaParams = new mutable.HashMap[String,Object]()
    kafkaParams += "bootstrap.servers" -> "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092"
    kafkaParams += "group.id" -> "group20"
    kafkaParams += "key.deserializer" -> classOf[StringDeserializer].getName
    kafkaParams += "value.deserializer" -> classOf[StringDeserializer].getName
    kafkaParams += "auto.offset.reset" -> "earliest"
    kafkaParams += "enable.auto.commit" -> "true"
    // 设置位置策略为 executor 与 kafka分区均分
    val location: LocationStrategy = LocationStrategies.PreferConsistent
    // 设置消费策略为订阅方式
    val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(topics.split(",").toList, kafkaParams)

    // 创建kafka直连流
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, location, consumerStrategy)
    val reduceDS: DStream[(String, Int)] = kafkaDS.flatMap(_.value().split(" ")).map((_,1)).reduceByKey(_ + _)
    reduceDS.foreachRDD((rdd, t) =>{
      println(s"rdd 分区数：${rdd.getNumPartitions}")
      println(s"time:${t}, data:${rdd.collect.toBuffer}")
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
