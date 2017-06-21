package com.redislabs.provider.redis

import com.wandoulabs.jodis.{RoundRobinJedisPool, JedisResourcePool}
import redis.clients.jedis.{JedisPoolConfig, Jedis, JedisPool}
import redis.clients.jedis.exceptions.JedisConnectionException

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._

/**
  * Redis Connection Pool
  */
object ConnectionPool {

  @transient private lazy val pools: ConcurrentHashMap[RedisEndpoint, JedisPool] =
    new ConcurrentHashMap[RedisEndpoint, JedisPool]()

  @transient private lazy val codisPools: ConcurrentHashMap[RedisEndpoint, JedisResourcePool] =
    new ConcurrentHashMap[RedisEndpoint, JedisResourcePool]()

  def connect(re: RedisEndpoint): Jedis = {
    val pool = pools.getOrElseUpdate(re,
      {
        val poolConfig: JedisPoolConfig = new JedisPoolConfig()
        poolConfig.setMaxTotal(250)
        poolConfig.setMaxIdle(32)
        poolConfig.setTestOnBorrow(false)
        poolConfig.setTestOnReturn(false)
        poolConfig.setTestWhileIdle(false)
        poolConfig.setMinEvictableIdleTimeMillis(60000)
        poolConfig.setTimeBetweenEvictionRunsMillis(30000)
        poolConfig.setNumTestsPerEvictionRun(-1)
        new JedisPool(poolConfig, re.host, re.port, re.timeout, re.auth, re.dbNum)
      }
    )

    var sleepTime: Int = 4
    var conn: Jedis = null

    while (conn == null) {
      try {
        conn = pool.getResource
      } catch {
        case e: JedisConnectionException if e.getCause.toString.
          contains("ERR max number of clients reached") => {
          if (sleepTime < 500) sleepTime *= 2
          Thread.sleep(sleepTime)
        }
        case e: Exception => throw e
      }
    }
    conn
  }

  def connectCodis(re: RedisEndpoint): Jedis = {
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(250)
    poolConfig.setMaxIdle(32)
    poolConfig.setTestOnBorrow(false)
    poolConfig.setTestOnReturn(false)
    poolConfig.setTestWhileIdle(false)
    poolConfig.setMinEvictableIdleTimeMillis(60000)
    poolConfig.setTimeBetweenEvictionRunsMillis(30000)
    poolConfig.setNumTestsPerEvictionRun(-1)

    // use codis poll
    val pool: JedisResourcePool = codisPools.getOrElseUpdate(re, RoundRobinJedisPool.create
      .curatorClient(re.zkHost, re.zkTimeout)
      .zkProxyDir(re.zkDir)
      .poolConfig(poolConfig)
      .build()
    )
    var sleepTime: Int = 4
    var conn: Jedis = null

    while (conn == null) {
      try {
        conn = pool.getResource
        if(!re.auth.equals("")) conn.auth(re.auth)

      } catch {
        case e: JedisConnectionException if e.getCause.toString.
          contains("ERR max number of clients reached") => {
          if (sleepTime < 500) sleepTime *= 2
          Thread.sleep(sleepTime)
        }
        case e: Exception => throw e
      }
    }
    conn
  }
}

