package com.hainiu.spark.db

import redis.clients.jedis.{JedisCluster, JedisPoolConfig}

object HainiuJedisClusterConnectionPool {

  private val config = new JedisPoolConfig
  //最大的连接数
  config.setMaxTotal(20)

  //最大的空闲数
  config.setMaxIdle(10)

  //保持连接活跃
  config.setTestOnBorrow(true)

  private val pool = new HainiuJedisClusterPool(config)

  def getConnection():JedisCluster = {
    pool.getResource
  }

  def close(j:JedisCluster) = {
    pool.returnResource(j)
  }


  def main(args: Array[String]): Unit = {
    //拿了两个连接
    val cluster: JedisCluster = HainiuJedisClusterConnectionPool.getConnection()
    val cluster1: JedisCluster = HainiuJedisClusterConnectionPool.getConnection()

    println(cluster.get("hainiu1"))

    //当前活跃连接数为2
    println(pool.getNumActive)

    //还连接到连接池里面
    HainiuJedisClusterConnectionPool.close(cluster)
    HainiuJedisClusterConnectionPool.close(cluster1)

    //当前活跃连接数为0
    println(pool.getNumActive)
    val cluster2: JedisCluster = HainiuJedisClusterConnectionPool.getConnection()
    val cluster3: JedisCluster = HainiuJedisClusterConnectionPool.getConnection()
    val cluster4: JedisCluster = HainiuJedisClusterConnectionPool.getConnection()

    //又拿了3个连接，这时活跃连接数为3，这里从池里拿了两个以前的，然后自己创建了一个新的
    //可以debug看一下GenericObjectPool的allObjects属性，这个allObjects属性里的对象数即为总的连接数
    println(pool.getNumActive)
  }
}
