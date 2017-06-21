package com.redislabs.provider.redis.example

/**
  * Created by zhengqh on 17/6/20.
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import com.redislabs.provider.redis._
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._
import com.wandoulabs.jodis.{JedisResourcePool, RoundRobinJedisPool}

object TestSparkRedis {
  /*
  val conf: SparkConf = new SparkConf()
    .setAppName("TestSparkRedis")
    .setMaster("local")
    .set("redis.host", "localhost")
    .set("redis.port", "6379")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext._
  import sqlContext.implicits._
  */

  val redisTimeout = 30000
  val zkHost: String = "192.168.6.55:2181,192.168.6.56:2181,192.168.6.57:2181"
  val zkProxyDir: String = "/zk/codis/db_tongdun_codis_test/proxy"
  val password = "tongdun123"

  val spark = SparkSession.builder().
    appName("TestSparkRedis").
    master("local").
    config("redis.host", "localhost").
    config("redis.port", "6379").
    config("codis.zk.host", zkHost).
    config("codis.zk.dir", zkProxyDir).
    config("codis.zk.timeout", "30000").
    config("redis.auth", password).
    //enableHiveSupport().
    getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    sparkRedis()
//    sparkWithPureCodis()
  }

  def sparkWithPureCodis(): Unit = {
    val wc = sc.parallelize(List(
      ("hello", "1"),
      ("world", "2"),
      ("redis", "2")
    ))

    wc.foreachPartition(partition=>{
      val jedisPool: JedisResourcePool = RoundRobinJedisPool.create.curatorClient(zkHost, redisTimeout).zkProxyDir(zkProxyDir).build
      val jedis: Jedis = jedisPool.getResource
      jedis.auth(password)

      partition.foreach(record => {
        jedis.zadd("zset", record._2.toLong, record._1)
      })
      jedis.close()
    })
  }

  def sparkRedis(): Unit = {
    val wc = sc.parallelize(List(
      ("hello", "1"),
      ("world", "2"),
      ("redis", "2")
    ), 1)

    val conf = sc.getConf
    println("zkhost:"+conf.get("codis.zk.host"))

    sc.toRedisKV(wc)
    sc.toRedisZSET(wc, "zkey1")
    sc.toRedisZSET(wc, "zkey2")
    sc.toRedisHASH(wc, "zmap1")
    sc.toRedisHASH(wc, "zmap2")
    sc.toRedisLIST(wc.map(_._1), "zlist1")
    sc.toRedisLIST(wc.map(_._1), "zlist2")

    val resultSet:RDD[(String, String)] = sc.fromRedisKV("*o*")
    resultSet.toDF().show
  }

  def sparkStreamingRedis(): Unit = {

  }

  // what if I want to implements keys in rdd. such as rdd = RDD[(String,String,String)]
  // the first column is used as key, other columns as data
  // say (12345, td, 12345), (12346, td, 12346) save as zset
  // zrange 12345 0 -1
  // zrange 12346 0 -1
  def batchSave(): Unit = {
    val wc = sc.parallelize(List(
      ("hello", "1")
    ))
    sc.toRedisHASH(wc, "map1")
    sc.toRedisHASH(wc, "map2")
    sc.toRedisHASH(wc, "map3")

    val wcBatch = sc.parallelize(List(
      ("map1", "hello", "1"),
      ("map2", "hello", "1"),
      ("map3", "hello", "1")
    ))

  }

  def pipeline(): Unit = {
    //要写入redis的数据，RDD[Map[String,String]]
    val rdd = sc.parallelize(List(
      ("map1", "key1", "111"),
      ("map1", "key2", "112"),
      ("map1", "key3", "113"),
      ("map2", "key1", "114")
    ))
    //使用pipeline 更高效的批处理
    // HSET map1 key1 value1
    // HSET map1 key2 value2
    rdd.foreachPartition{iter =>
      val redis = new Jedis("localhost",6379,400000)
      val ppl = redis.pipelined()
      iter.foreach{row =>
        val mapKey = row._1
        val key = row._2
        val value = row._3
        ppl.hmset(mapKey, Map(key -> value))

        //zadd key score member
        ppl.zadd(mapKey.replace("map","set"), value.toDouble, key)
      }
      ppl.sync()
    }

    //TODO HMSET map1 key1 value1 key2 value2
  }


}
