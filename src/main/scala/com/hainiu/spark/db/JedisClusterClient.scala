package com.hainiu.spark.db

import java.util

import redis.clients.jedis.{HostAndPort, JedisCluster}

object JedisClusterClient {
  def main(args: Array[String]): Unit = {

    val jedisClusters = new util.HashSet[HostAndPort]
    jedisClusters.add(new HostAndPort("nn1.hadoop",6379))
    jedisClusters.add(new HostAndPort("nn2.hadoop",6379))
    jedisClusters.add(new HostAndPort("s1.hadoop",6379))
    jedisClusters.add(new HostAndPort("s2.hadoop",6379))
    jedisClusters.add(new HostAndPort("s3.hadoop",6379))
    jedisClusters.add(new HostAndPort("s4.hadoop",6379))

    val jedisCluster = new JedisCluster(jedisClusters)
    val str: String = jedisCluster.get("hainiu1")
    println(str)

  }
}
