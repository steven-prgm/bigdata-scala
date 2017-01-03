package com.boost.bigdata.utils.db

import java.util
import java.util.{HashSet, Set}

import com.boost.bigdata.utils.log.LogSupport
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, JedisPool, JedisPoolConfig, ShardedJedis}
import redis.clients.util.Pool

object JedisManager extends LogSupport {

  /**
    * create a one node only jedisCluster client connection
    */
  def createJedisCluster(server: String, port: Int): JedisCluster = {
    var jedis_cluster: JedisCluster = null
    try {
      val nodes: Set[HostAndPort] = new HashSet[HostAndPort]
      nodes.add(new HostAndPort(server, port))
      jedis_cluster = new JedisCluster(nodes)
    } catch {
      case e: Exception => {
        log.warn(e.getMessage, e)
      }
    }
    jedis_cluster
  }

  /**
   * create jedisCluster client connection
   * input format : server:port,...  (192.168.1.25:6379,192.168.1.26:6379,192.168.1.27:6379)
   *
   */

  def createJedisCluster(services: String): JedisCluster = {
    var jedis_cluster: JedisCluster = null
    try {
      val nodes: Set[HostAndPort] = new HashSet[HostAndPort]
      val servicesList: Array[String] = services.split(",")
      servicesList.map(service => {
        val config = service.split(":")
        nodes.add(new HostAndPort(config.head, config.last.toInt))
      })
      jedis_cluster = new JedisCluster(nodes)
    } catch {
      case e: Exception => {
        log.warn(e.getMessage, e)
      }
    }
    jedis_cluster
  }

  def createJedis(server: String, port: Int): Jedis = {
    var jedis: Jedis = null
    try {
      jedis = new Jedis(server, port)
      jedis.connect()
    } catch {
      case e: Exception => log.warn(e.getMessage, e)
        jedis
    }
    jedis
  }

  def createJedisPool(): Pool[Jedis] = {
    val config = new JedisPoolConfig()
    //    config.setMaxTotal(conf.getInt("jedis.pool.max.resources", 100))
    //    config.setMaxIdle(conf.getInt("jedis.pool.max.idle", 40))
    //    config.setMaxWaitMillis(conf.getInt("jedis.pool.get_resource.timeout", 10 * 1000))
    config.setMaxTotal(100)
    config.setMaxIdle(40)
    config.setMaxWaitMillis(10 * 1000)
    config.setTestOnBorrow(true)

    var pool: Pool[Jedis] = null

    try {
      /*String redis_server = conf.getString("redis.server");
      int redis_port = conf.getInt("redis.port");
      String redis_passwd = conf.getString("redis.password");
      int connection_timeout = conf.getInt("redis.connection.timeout", 10 * 1000);*/
      val redis_server = "127.0.0.1" //conf.getString("redis.server");
      val redis_port = 8081 //conf.getInt("redis.port");
      val redis_passwd = "" //conf.getString("redis.password");
      val connection_timeout = 10 * 1000 //conf.getInt("redis.connection.timeout", 10 * 1000);

      if (!redis_passwd.isEmpty) {
        pool = new JedisPool(config, redis_server, redis_port, connection_timeout)
      } else {
        pool = new JedisPool(config, redis_server, redis_port, connection_timeout, redis_passwd)
      }

      if (!validatePool(pool)) {
        closeJedisPool(pool)
        pool = null
      }
    } catch {
      case e: Exception => log.warn(e.getMessage)
        closeJedisPool(pool)
        pool = null
    }
    pool
  }

  def returnSource(pool: Pool[Jedis], jedis: Jedis, is_ok: Boolean): Unit = {
    if (pool != null) {
      if (is_ok) {
        try {
          pool.returnResource(jedis)
        } catch {
          case e: Exception => log.warn(e.getMessage)
        }
      } else {
        try {
          pool.returnBrokenResource(jedis)
        } catch {
          case e: Exception => log.warn(e.getMessage)
        }
      }
    }
  }

  def returnSource(pool: Pool[ShardedJedis], jedis: ShardedJedis, is_ok: Boolean): Unit = {
    if (pool != null) {
      if (is_ok) {
        try {
          pool.returnResource(jedis)
        } catch {
          case e: Exception => log.warn(e.getMessage)
        }
      } else {
        try {
          pool.returnBrokenResource(jedis)
        } catch {
          case e: Exception => log.warn(e.getMessage)
        }
      }
    }
  }

  def validatePool(pool: Pool[Jedis]): Boolean = {
    var ret = false
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      ret = jedis != null
    } catch {
      case e: Exception => log.error(e.getMessage, e.printStackTrace())
    }

    if (ret) {
      returnSource(pool, jedis, ret)
    }
    ret
  }

  def closeJedis(jedis: Jedis): Unit = {
    if (jedis != null) {
      try {
        jedis.disconnect()
      } catch {
        case e: Exception => log.warn(e.getMessage, e)
      }
    }
  }

  def closeJedisCluster(jedis_cluster: JedisCluster): Unit = {
    if (jedis_cluster != null) {
      try {
        jedis_cluster.close()
      } catch {
        case e: Exception => log.warn(e.getMessage, e)
      }
    }
  }

  def closeJedisPool(pool: Pool[Jedis]): Unit = {
    if (pool != null) {
      try {
        pool.close()
      } catch {
        case e: Exception => log.warn(e.getMessage)
      }
    }
  }

  def consumeListByRangeSync(jedis: Jedis, redis_key: String, start: Long, end: Long): util.List[String] = {
    val result: util.List[String] = jedis.lrange(redis_key, start, end)
    if (result != null) {
      jedis.ltrim(redis_key, result.size(), -1)
    }
    result
  }
}
