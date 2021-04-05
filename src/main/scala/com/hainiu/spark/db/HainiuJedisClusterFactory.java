package com.hainiu.spark.db;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

public class HainiuJedisClusterFactory implements PooledObjectFactory<JedisCluster> {

    public PooledObject<JedisCluster> makeObject() throws Exception {
        Set jedisClusters = new HashSet<HostAndPort>();
        jedisClusters.add(new HostAndPort("nn1.hadoop",6379));
        jedisClusters.add(new HostAndPort("nn2.hadoop",6379));
        jedisClusters.add(new HostAndPort("s1.hadoop",6379));
        jedisClusters.add(new HostAndPort("s2.hadoop",6379));
        jedisClusters.add(new HostAndPort("s3.hadoop",6379));
        jedisClusters.add(new HostAndPort("s4.hadoop",6379));
        JedisCluster jedisCluster = new JedisCluster(jedisClusters);
        return new DefaultPooledObject<JedisCluster>(jedisCluster);
    }

    public void destroyObject(PooledObject<JedisCluster> p) throws Exception {

    }

    public boolean validateObject(PooledObject<JedisCluster> p) {
        return true;
    }

    public void activateObject(PooledObject<JedisCluster> p) throws Exception {

    }

    public void passivateObject(PooledObject<JedisCluster> p) throws Exception {

    }
}
