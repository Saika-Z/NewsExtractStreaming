package com.hainiu.spark.db;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.util.Pool;

public class HainiuJedisClusterPool  extends Pool<JedisCluster> {

    public HainiuJedisClusterPool(GenericObjectPoolConfig poolConfig){
        super(poolConfig,new HainiuJedisClusterFactory());
    }

    @Override
    public JedisCluster getResource() {
        JedisCluster jedisCluster = super.getResource();
        return jedisCluster;
    }

}
